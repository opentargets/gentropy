"""Integration test of the FinnGen study index step's orchestration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.finngen_studies import FinnGenStudiesStep


class TestFinnGenStudiesStepIntegration:
    """Test that FinnGenStudiesStep wires the EFO mapping, the study index and the write.

    The study index building and the EFO annotation are mocked out; their logic is covered by
    the FinnGen datasource tests. Only the step's own orchestration is verified: the release
    prefix handed on, the arguments of each collaborator, and the output written to disk.
    """

    @pytest.mark.step_test
    @patch("gentropy.finngen_studies.FinnGenStudyIndex.from_source")
    @patch("gentropy.finngen_studies.EFOMapping.from_path")
    def test_orchestration(
        self,
        mock_efo_from_path: MagicMock,
        mock_study_index_from_source: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """The study index is built, annotated with the EFO mapping of the release, and written."""
        output_path = tmp_path / "study_index"
        study_index = MagicMock(name="study_index")
        mock_study_index_from_source.return_value = study_index
        annotated = MagicMock(name="annotated_study_index")
        annotated.df = session.spark.createDataFrame(
            [("FINNGEN_R12_AB1_EBV", "gwas"), ("FINNGEN_R12_T2D", "gwas")],
            "studyId string, studyType string",
        )
        mock_efo_from_path.return_value.annotate_study_index.return_value = annotated

        FinnGenStudiesStep(
            session=session,
            finngen_study_index_out=str(output_path),
            finngen_phenotype_table_url="gs://bucket/phenotypes.json",
            # The trailing underscore is removed before the prefix is used:
            finngen_release_prefix="FINNGEN_R12_",
            finngen_summary_stats_url_prefix="gs://bucket/summary_stats/finngen_R12_",
            finngen_summary_stats_url_suffix=".gz",
            efo_curation_mapping_url="gs://bucket/efo_mapping.tsv",
            sample_size=500_348,
        )

        mock_efo_from_path.assert_called_once_with(
            session, "gs://bucket/efo_mapping.tsv"
        )
        mock_study_index_from_source.assert_called_once_with(
            session.spark,
            "gs://bucket/phenotypes.json",
            "FINNGEN_R12",
            "gs://bucket/summary_stats/finngen_R12_",
            ".gz",
            500_348,
        )
        mock_efo_from_path.return_value.annotate_study_index.assert_called_once_with(
            study_index, "R12"
        )
        written = session.spark.read.parquet(str(output_path))
        assert sorted(row.studyId for row in written.collect()) == [
            "FINNGEN_R12_AB1_EBV",
            "FINNGEN_R12_T2D",
        ]

    @pytest.mark.step_test
    def test_invalid_release_prefix_is_rejected(
        self, session: Session, tmp_path: Path
    ) -> None:
        """A release prefix not shaped like FINNGEN_R* stops the step before reading anything."""
        with pytest.raises(ValueError, match="Invalid FinnGen release prefix"):
            FinnGenStudiesStep(
                session=session,
                finngen_study_index_out=str(tmp_path / "study_index"),
                finngen_phenotype_table_url="gs://bucket/phenotypes.json",
                finngen_release_prefix="R12",
                efo_curation_mapping_url="gs://bucket/efo_mapping.tsv",
            )
