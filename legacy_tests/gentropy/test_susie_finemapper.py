"""Tests for SusieFineMapperStep."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gentropy.common.session import Session
from gentropy.susie_finemapper import SusieFineMapperStep


@pytest.mark.step_test
class TestSusieFineMapperStep:
    """Test SusieFineMapperStep initialization and helper methods."""

    def test_empty_log_mg_creates_file(self, tmp_path: Path) -> None:
        """Test that _empty_log_mg creates a CSV file with correct structure."""
        output_path = str(tmp_path / "test_log.tsv")

        SusieFineMapperStep._empty_log_mg(
            studyId="STUDY001",
            region="chr1:1000-2000",
            error_mg="Test error message",
            path_out=output_path,
        )

        # Verify file was created
        assert Path(output_path).exists()

        # Read and verify content
        df = pd.read_csv(output_path, sep="\t")
        assert df.shape[0] == 1
        assert df.loc[0, "studyId"] == "STUDY001"
        assert df.loc[0, "region"] == "chr1:1000-2000"
        assert df.loc[0, "error"] == "Test error message"

    def test_empty_log_mg_column_structure(self, tmp_path: Path) -> None:
        """Test that _empty_log_mg creates all expected columns."""
        output_path = str(tmp_path / "test_log_columns.tsv")

        SusieFineMapperStep._empty_log_mg(
            studyId="TEST_STUDY",
            region="chr10:5000-6000",
            error_mg="Some error",
            path_out=output_path,
        )

        df = pd.read_csv(output_path, sep="\t")

        expected_columns = {
            "studyId",
            "region",
            "N_gwas_before_dedupl",
            "N_gwas",
            "N_ld",
            "N_overlap",
            "N_outliers",
            "N_imputed",
            "N_final_to_fm",
            "elapsed_time",
            "number_of_CS",
            "error",
        }

        assert set(df.columns) == expected_columns

    def test_empty_log_mg_default_numeric_values(self, tmp_path: Path) -> None:
        """Test that _empty_log_mg sets all numeric fields to 0."""
        output_path = str(tmp_path / "test_log_values.tsv")

        SusieFineMapperStep._empty_log_mg(
            studyId="STUDY_NUM",
            region="chr5:1000-2000",
            error_mg="Error",
            path_out=output_path,
        )

        df = pd.read_csv(output_path, sep="\t")

        numeric_columns = {
            "N_gwas_before_dedupl",
            "N_gwas",
            "N_ld",
            "N_overlap",
            "N_outliers",
            "N_imputed",
            "N_final_to_fm",
            "elapsed_time",
            "number_of_CS",
        }

        for col in numeric_columns:
            assert df.loc[0, col] == 0, f"Column {col} should be 0"

    def test_empty_log_mg_different_study_ids(self, tmp_path: Path) -> None:
        """Test _empty_log_mg with various study IDs."""
        study_ids = ["STUDY_A", "STUDY_123", "ST_XYZ"]

        for study_id in study_ids:
            output_path = str(tmp_path / f"{study_id}_log.tsv")
            SusieFineMapperStep._empty_log_mg(
                studyId=study_id,
                region="chr1:1-100",
                error_mg="Error",
                path_out=output_path,
            )

            df = pd.read_csv(output_path, sep="\t")
            assert df.loc[0, "studyId"] == study_id

    def test_empty_log_mg_special_characters_in_error(self, tmp_path: Path) -> None:
        """Test _empty_log_mg with special characters in error message."""
        output_path = str(tmp_path / "test_special_chars.tsv")
        error_msg = "Error: File not found (path=/data/test)"

        SusieFineMapperStep._empty_log_mg(
            studyId="STUDY",
            region="chr1:1-100",
            error_mg=error_msg,
            path_out=output_path,
        )

        df = pd.read_csv(output_path, sep="\t")
        assert df.loc[0, "error"] == error_msg

    def test_susie_fine_mapper_step_initialization_fails_without_manifest(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that SusieFineMapperStep raises error when manifest doesn't exist."""
        missing_manifest = str(tmp_path / "missing_manifest.csv")

        with pytest.raises(FileNotFoundError):
            SusieFineMapperStep(
                session=session,
                study_index_path=str(tmp_path / "study_index"),
                study_locus_manifest_path=missing_manifest,
                study_locus_index=0,
                ld_matrix_paths={},
            )

    def test_susie_fine_mapper_step_initialization_with_manifest(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that SusieFineMapperStep can be initialized with valid manifest."""
        # Create a minimal manifest file
        manifest_data = pd.DataFrame(
            {
                "study_locus_input": [str(tmp_path / "input")],
                "study_locus_output": [str(tmp_path / "output")],
            }
        )
        manifest_path = str(tmp_path / "manifest.csv")
        manifest_data.to_csv(manifest_path, index=False)

        with (
            patch(
                "gentropy.susie_finemapper.StudyLocus.from_parquet"
            ) as mock_study_locus,
            patch(
                "gentropy.susie_finemapper.StudyIndex.from_parquet"
            ) as mock_study_index,
        ):
            # Mock the study locus and index
            mock_sl = MagicMock()
            mock_sl.df.withColumn.return_value.collect.return_value = [MagicMock()]
            mock_study_locus.return_value = mock_sl

            mock_study_index.return_value = MagicMock()

            with patch(
                "gentropy.susie_finemapper.SusieFineMapperStep.susie_finemapper_one_sl_row_gathered_boundaries"
            ) as mock_finemapper:
                mock_finemapper.return_value = None

                step = SusieFineMapperStep(
                    session=session,
                    study_index_path=str(tmp_path / "study_index"),
                    study_locus_manifest_path=manifest_path,
                    study_locus_index=0,
                    ld_matrix_paths={},
                )

                assert step is not None

    def test_susie_fine_mapper_step_invalid_index(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that SusieFineMapperStep raises error with invalid index."""
        manifest_data = pd.DataFrame(
            {
                "study_locus_input": [str(tmp_path / "input")],
                "study_locus_output": [str(tmp_path / "output")],
            }
        )
        manifest_path = str(tmp_path / "manifest.csv")
        manifest_data.to_csv(manifest_path, index=False)

        with pytest.raises(Exception):  # IndexError or similar
            SusieFineMapperStep(
                session=session,
                study_index_path=str(tmp_path / "study_index"),
                study_locus_manifest_path=manifest_path,
                study_locus_index=999,  # Out of bounds
                ld_matrix_paths={},
            )

    def test_susie_fine_mapper_step_initialization_parameters(self) -> None:
        """Test that SusieFineMapperStep has correct expected parameters."""
        import inspect

        sig = inspect.signature(SusieFineMapperStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "study_index_path",
            "study_locus_manifest_path",
            "study_locus_index",
            "ld_matrix_paths",
            "max_causal_snps",
            "lead_pval_threshold",
            "purity_mean_r2_threshold",
            "purity_min_r2_threshold",
            "cs_lbf_thr",
            "sum_pips",
            "susie_est_tausq",
            "run_carma",
            "run_sumstat_imputation",
            "carma_time_limit",
            "carma_tau",
            "imputed_r2_threshold",
            "ld_score_threshold",
            "ld_min_r2",
            "ignore_qc",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"

    def test_susie_fine_mapper_step_default_parameters(self) -> None:
        """Test that SusieFineMapperStep has correct default parameter values."""
        import inspect

        sig = inspect.signature(SusieFineMapperStep.__init__)

        # Check default values
        assert sig.parameters["max_causal_snps"].default == 10
        assert sig.parameters["lead_pval_threshold"].default == 1e-5
        assert sig.parameters["purity_mean_r2_threshold"].default == 0
        assert sig.parameters["purity_min_r2_threshold"].default == 0.25
        assert sig.parameters["cs_lbf_thr"].default == 2
        assert sig.parameters["sum_pips"].default == 0.99
        assert sig.parameters["susie_est_tausq"].default is False
        assert sig.parameters["run_carma"].default is False
        assert sig.parameters["run_sumstat_imputation"].default is False
        assert sig.parameters["carma_time_limit"].default == 600
        assert sig.parameters["carma_tau"].default == 0.15
        assert sig.parameters["imputed_r2_threshold"].default == 0.9
        assert sig.parameters["ld_score_threshold"].default == 5
        assert sig.parameters["ld_min_r2"].default == 0.8
        assert sig.parameters["ignore_qc"].default is False

    @pytest.mark.parametrize(
        "region,study_id",
        [
            ("chr1:1000-2000", "STUDY_1"),
            ("chr22:500000-600000", "STUDY_22"),
            ("chrX:100-200", "STUDY_X"),
        ],
    )
    def test_empty_log_mg_parametrized(
        self, tmp_path: Path, region: str, study_id: str
    ) -> None:
        """Test _empty_log_mg with various region and study ID combinations."""
        output_path = str(tmp_path / f"{study_id}_log.tsv")

        SusieFineMapperStep._empty_log_mg(
            studyId=study_id,
            region=region,
            error_mg="Test error",
            path_out=output_path,
        )

        df = pd.read_csv(output_path, sep="\t")
        assert df.loc[0, "studyId"] == study_id
        assert df.loc[0, "region"] == region
