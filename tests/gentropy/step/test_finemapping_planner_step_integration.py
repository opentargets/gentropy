"""Mocked integration test for the fine-mapping plan generator step's real write path."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.dataset.fine_mapping import FineMappingRoute
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.finemapping_planner import FineMappingPlanGeneratorStep

STUDY_REQUIRED_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, "
    "hasSumstats BOOLEAN, qualityControls ARRAY<STRING>, analysisFlags ARRAY<STRING>, "
    "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>, "
    "traitFromSourceMappedIds ARRAY<STRING>, nSamples INT, nCases INT, nControls INT"
)


class TestFineMappingPlanGeneratorStepIntegration:
    """Mocked integration test: mocks StudyIndex construction, but exercises real Spark resolve() + write.

    StudyIndex is patched as imported into gentropy.finemapping_planner so we can attach a
    small real in-memory Spark DataFrame in place of a parquet read, while MultiSuSiEConstraintSet
    and the write path (repartition/write.mode/partitionBy/parquet) run for real against tmp_path.
    """

    @pytest.mark.step_test
    @patch("gentropy.finemapping_planner.StudyIndex")
    def test_step_writes_real_parquet_output(
        self,
        study_index: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Test that the step writes a real, route-partitioned parquet plan from a small real study index."""
        input_path = "unused_input_path"
        output_path = (tmp_path / "fine_mapping_plan").as_posix()

        data: list[tuple[object, ...]] = [
            (
                "eligible_nfe",
                "p",
                StudyType.GWAS.value,
                True,
                [],
                [],
                [("nfe", 1.0)],
                ["EFO_1"],
                100_000,
                None,
                None,
            ),
            (
                "eligible_afr",
                "p",
                StudyType.GWAS.value,
                True,
                [],
                [],
                [("afr", 1.0)],
                ["EFO_1"],
                50_000,
                None,
                None,
            ),
            (
                "ineligible_no_sumstats",
                "p",
                StudyType.GWAS.value,
                False,
                [],
                [],
                [("csa", 1.0)],
                ["EFO_2"],
                10_000,
                None,
                None,
            ),
        ]
        real_df = session.spark.createDataFrame(data, STUDY_REQUIRED_SCHEMA)
        real_study_index = StudyIndex(_df=real_df)

        # from_parquet returns a genuine StudyIndex (wrapping the real, small in-memory
        # DataFrame above) rather than a mock, so resolve()'s real Spark logic - including
        # validate_ccs() - runs against real data. Only the parquet *read* is mocked out.
        study_index.from_parquet = MagicMock(return_value=real_study_index)

        FineMappingPlanGeneratorStep(
            session, input_path=input_path, output_path=output_path
        )

        study_index.from_parquet.assert_called_once_with(session, input_path)

        written_df = session.spark.read.parquet(output_path)

        # Written as parquet, partitioned by route, with the expected schema.
        assert set(written_df.columns) == {"studyId", "runId", "constraints", "route"}
        assert (
            written_df.select("route").distinct().collect()[0]["route"]
            == FineMappingRoute.MULTI_SUSIE_ROUTE.value
        )

        # All three input studies are represented in the plan (ineligible studies are kept, not dropped).
        assert {row["studyId"] for row in written_df.select("studyId").collect()} == {
            "eligible_nfe",
            "eligible_afr",
            "ineligible_no_sumstats",
        }

        # The two eligible, cross-ancestry studies for the same trait share a runId.
        rows_by_id = {row["studyId"]: row for row in written_df.collect()}
        assert rows_by_id["eligible_nfe"]["runId"] is not None
        assert (
            rows_by_id["eligible_nfe"]["runId"] == rows_by_id["eligible_afr"]["runId"]
        )
        assert rows_by_id["ineligible_no_sumstats"]["runId"] is None
