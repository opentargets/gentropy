"""Test study qc step."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row

from gentropy import Session
from gentropy.study_validation import StudyValidationStep


class TestStudyQcStep:
    """Test study qc step."""

    @pytest.mark.step_test
    @patch("pyspark.sql.readwriter.DataFrameReader.parquet")
    @patch("gentropy.study_validation.TargetIndex")
    @patch("gentropy.study_validation.BiosampleIndex")
    @patch("gentropy.study_validation.StudyIndex")
    def test_step(
        self,
        study_index: MagicMock,
        biosample_index: MagicMock,
        target_index: MagicMock,
        parquet: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Test run method."""
        study_index_paths = ["study_index_path"]
        target_index_path = "target_index_path"
        disease_index_path = "disease_index_path"
        biosample_index_path = "biosample_index_path"
        valid_study_index_path = (tmp_path / "valid_study_index_path").as_posix()
        invalid_study_index_path = (tmp_path / "invalid_study_index_path").as_posix()
        invalid_qc_reasons = ["reason1", "reason2"]
        deprecated_project_ids = ["proj1", "proj2"]

        # Make from_parquet return the patched class mock itself so
        # instance-style methods (deconvolute_studies, validate_*, etc.)
        # are available on the returned object.
        target_index.from_parquet = MagicMock(return_value=target_index)
        biosample_index.from_parquet = MagicMock(return_value=biosample_index)
        study_index.from_parquet = MagicMock(return_value=study_index)

        # Mock disease index dataframe

        disease_df = session.spark.createDataFrame(
            [Row(id="d1", obsoleteTerms=["o1"]), Row(id="d2", obsoleteTerms=["o2"])]
        )
        parquet.return_value = disease_df

        # Mock StudyIndex validation methods
        study_index.deconvolute_studies = MagicMock(return_value=study_index)
        study_index.validate_study_type = MagicMock(return_value=study_index)
        study_index.validate_project_id = MagicMock(return_value=study_index)
        study_index.validate_target = MagicMock(return_value=study_index)
        study_index.validate_disease = MagicMock(return_value=study_index)
        study_index.validate_biosample = MagicMock(return_value=study_index)
        study_index.validate_analysis_flags = MagicMock(return_value=study_index)
        study_index.persist = MagicMock(return_value=study_index)
        study_index.valid_rows = MagicMock(return_value=study_index)

        si_df = session.spark.createDataFrame([Row(studyId="s1"), Row(studyId="s2")])

        study_index.df = si_df

        StudyValidationStep(
            session,
            study_index_path=study_index_paths,
            target_index_path=target_index_path,
            disease_index_path=disease_index_path,
            biosample_index_path=biosample_index_path,
            valid_study_index_path=valid_study_index_path,
            invalid_study_index_path=invalid_study_index_path,
            invalid_qc_reasons=invalid_qc_reasons,
            deprecated_project_ids=deprecated_project_ids,
        )

        # Assert reading datasets with correct paths
        target_index.from_parquet.assert_called_once_with(session, target_index_path)
        biosample_index.from_parquet.assert_called_once_with(
            session, biosample_index_path
        )
        study_index.from_parquet.assert_called_once_with(session, study_index_paths)

        # Assert reading disease index
        parquet.assert_called_once_with(disease_index_path)

        # Assert validation methods called
        study_index.deconvolute_studies.assert_called_once()
        study_index.validate_study_type.assert_called_once()
        study_index.validate_project_id.assert_called_once_with(deprecated_project_ids)
        study_index.validate_target.assert_called_once()
        study_index.validate_disease.assert_called_once()
        study_index.validate_biosample.assert_called_once()
        study_index.validate_analysis_flags.assert_called_once()

        # Assert valid_rows
        study_index.valid_rows.assert_any_call(invalid_qc_reasons, invalid=True)
        study_index.valid_rows.assert_any_call(invalid_qc_reasons)
