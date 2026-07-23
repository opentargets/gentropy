"""Integration test for the fine-mapping manifest generator step's real generate + write path."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.dataset.fine_mapping import FineMappingManifest, FineMappingPlanner
from gentropy.dataset.study_index import StudyIndex
from gentropy.finemapping_manifest import GWASCatalogFineMappingManifestGenerator

# Minimal schema satisfying StudyIndex mandatory-column validation, plus the fields
# consumed by generate_manifest (summarystatsLocation).
_SI_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, hasSumstats BOOLEAN, "
    "qualityControls ARRAY<STRING>, analysisFlags ARRAY<STRING>, "
    "summarystatsLocation STRING, "
    "traitFromSourceMappedIds ARRAY<STRING>, "
    "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>, "
    "nSamples INT, nCases INT, nControls INT"
)

# Minimal schema for FineMappingPlanner fields consumed by generate_manifest.
_PLANNER_SCHEMA = (
    "runId STRING, studyId STRING, route STRING, "
    "constraints ARRAY<STRUCT<name:STRING,value:BOOLEAN>>"
)


class TestGWASCatalogFineMappingManifestGeneratorIntegration:
    """Integration test: patches only parquet reads, runs real Spark generate_manifest + write.

    StudyIndex and FineMappingPlanner are injected as real in-memory DataFrames so the
    full join / ancestry / trait-set logic executes against genuine data. The final TSV is
    written to a local tmp_path file via toPandas().
    """

    @pytest.mark.step_test
    @patch("gentropy.finemapping_manifest.FineMappingPlanner")
    @patch("gentropy.finemapping_manifest.StudyIndex")
    def test_manifest_tsv_written_with_expected_content(
        self,
        mock_si_cls: MagicMock,
        mock_planner_cls: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step writes a valid TSV with one row per planner entry that has summary statistics."""
        output_path = str(tmp_path / "manifest.tsv")

        # studyId, projectId, studyType, hasSumstats, qualityControls, analysisFlags,
        # summarystatsLocation, traitFromSourceMappedIds, ldPopulationStructure,
        # nSamples, nCases, nControls
        si_data: list[tuple[object, ...]] = [
            # eligible: GCST project, hasSumstats, non-null sumstats location, LD structure
            # measurement study design (nCases/nControls empty, nSamples > 0)
            (
                "GCST001",
                "GCST",
                "gwas",
                True,
                [],
                [],
                "gs://b/GCST001.tsv.gz",
                ["EFO_1"],
                [("nfe", 1.0)],
                100_000,
                None,
                None,
            ),
            # eligible: case-control study design (nCases + nControls == nSamples)
            (
                "GCST002",
                "GCST",
                "gwas",
                True,
                [],
                [],
                "gs://b/GCST002.tsv.gz",
                ["EFO_2", "EFO_3"],
                [("afr", 1.0)],
                50_000,
                20_000,
                30_000,
            ),
            # ineligible: wrong project
            (
                "OTHER001",
                "OTHER",
                "gwas",
                True,
                [],
                [],
                "gs://b/OTHER001.tsv.gz",
                ["EFO_4"],
                [("nfe", 1.0)],
                10_000,
                None,
                None,
            ),
        ]
        real_si = StudyIndex(
            _df=session.spark.createDataFrame(si_data, schema=_SI_SCHEMA)
        )
        mock_si_cls.from_parquet.return_value = real_si

        planner_data: list[tuple[object, ...]] = [
            ("run-abc", "GCST001", "multi_susie_route", []),
            ("run-abc", "GCST002", "multi_susie_route", []),
            # study present in planner but not in study index → dropped from result
            (None, "GCST999", "multi_susie_route", []),
        ]
        real_planner = FineMappingPlanner(
            _df=session.spark.createDataFrame(planner_data, schema=_PLANNER_SCHEMA)
        )
        mock_planner_cls.from_parquet.return_value = real_planner

        GWASCatalogFineMappingManifestGenerator(
            session=session,
            study_index_path="unused",
            fine_mapping_planner_path="unused",
            output_path=output_path,
        )

        assert Path(output_path).exists(), "TSV file was not created"

        import pandas as pd

        df = pd.read_csv(output_path, sep="\t")

        assert set(df.columns) == set(FineMappingManifest.get_schema().fieldNames())
        assert len(df) == 2, f"Expected 2 rows (GCST001 + GCST002), got {len(df)}"
        assert set(df["studyId"]) == {"GCST001", "GCST002"}
        assert set(df["route"]) == {"multi_susie_route"}
        assert set(df["runId"]) == {"run-abc"}

        # majorAncestry is derived from ldPopulationStructure
        ancestry_by_study = dict(zip(df["studyId"], df["majorAncestry"]))
        assert ancestry_by_study["GCST001"] == "nfe"
        assert ancestry_by_study["GCST002"] == "afr"

        # traitFromSourceMappedIds is deduped and sorted; written to TSV as a list repr
        trait_by_study = dict(zip(df["studyId"], df["traitFromSourceMappedIds"]))
        assert trait_by_study["GCST001"] == "['EFO_1']"
        assert trait_by_study["GCST002"] == "['EFO_2', 'EFO_3']"

        # effectiveSampleSize: measurement studies use nSamples; case-control studies
        # use the effective_sample_size formula (4 * cases * controls) / (cases + controls)
        ess_by_study = dict(zip(df["studyId"], df["effectiveSampleSize"]))
        assert ess_by_study["GCST001"] == 100_000
        assert ess_by_study["GCST002"] == int((4 * 20_000 * 30_000) / (20_000 + 30_000))

    @pytest.mark.step_test
    @patch("gentropy.finemapping_manifest.FineMappingPlanner")
    @patch("gentropy.finemapping_manifest.StudyIndex")
    def test_manifest_drops_studies_missing_from_study_index(
        self,
        mock_si_cls: MagicMock,
        mock_planner_cls: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Studies present in the planner but absent from the study index are omitted from output.

        This also exercises the warning code path (actual < expected row count) without
        relying on Log4j capture, which is not available via pytest's caplog fixture.
        """
        output_path = str(tmp_path / "manifest.tsv")

        si_data: list[tuple[object, ...]] = [
            (
                "GCST001",
                "GCST",
                "gwas",
                True,
                [],
                [],
                "gs://b/GCST001.tsv.gz",
                ["EFO_1"],
                [("nfe", 1.0)],
                100_000,
                None,
                None,
            ),
        ]
        real_si = StudyIndex(
            _df=session.spark.createDataFrame(si_data, schema=_SI_SCHEMA)
        )
        mock_si_cls.from_parquet.return_value = real_si

        # Planner has two studies but only one is in the study index.
        planner_data: list[tuple[object, ...]] = [
            ("run-abc", "GCST001", "multi_susie_route", []),
            ("run-xyz", "GCST_MISSING", "multi_susie_route", []),
        ]
        real_planner = FineMappingPlanner(
            _df=session.spark.createDataFrame(planner_data, schema=_PLANNER_SCHEMA)
        )
        mock_planner_cls.from_parquet.return_value = real_planner

        import pandas as pd

        # Spy on the Log4j warning call to confirm it fires.
        with patch.object(session.logger, "warning") as mock_warning:
            GWASCatalogFineMappingManifestGenerator(
                session=session,
                study_index_path="unused",
                fine_mapping_planner_path="unused",
                output_path=output_path,
            )

        df = pd.read_csv(output_path, sep="\t")
        assert len(df) == 1
        assert df["studyId"].iloc[0] == "GCST001"

        # The Log4j warning was triggered because actual < expected row count.
        mock_warning.assert_called_once()
        assert (
            "do not have corresponding summary statistics"
            in mock_warning.call_args[0][0]
        )
