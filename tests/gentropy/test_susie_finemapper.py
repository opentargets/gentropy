"""Tests for SusieFineMapperStep."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

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
            "sigmasq",
            "sigmasq_floored",
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


class TestSusieInfToStudyLocusStandardError:
    """`standardError` must reach the emitted credible sets.

    It is present in the harmonised summary statistics and used to build the
    z-scores that drive fine-mapping, but it used to be dropped on the way out:
    the locus struct was assembled without it and the top-level column was never
    set. That left every consumer needing an effect-size uncertainty to re-join
    the summary statistics, and removed the obvious clue that `beta` here is
    SuSiE's posterior mean rather than the marginal effect.
    """

    P_VARIANTS = 4
    STANDARD_ERRORS = [0.11, 0.22, 0.33, 0.44]

    @pytest.fixture()
    def susie_output(self) -> dict[str, object]:
        """A single-effect SuSiE-inf output over four variants."""
        import numpy as np

        return {
            "PIP": np.array([[0.90], [0.06], [0.03], [0.01]]),
            "lbf_variable": np.array([[10.0], [5.0], [3.0], [1.0]]),
            "mu": np.array([[0.5], [-0.3], [0.2], [0.1]]),
            "lbf": np.array([12.0]),
        }

    @pytest.fixture()
    def variant_index(self, spark: SparkSession) -> DataFrame:
        """Variant index carrying z and standardError, in SuSiE array order."""
        rows = [
            (f"1_{1000 + i}_A_G", float(8 - i), se, "1", 1000 + i)
            for i, se in enumerate(self.STANDARD_ERRORS)
        ]
        return spark.createDataFrame(
            rows,
            "variantId string, z double, standardError double, "
            "chromosome string, position int",
        ).repartition(1)

    def test_standard_error_reaches_locus_and_top_level(
        self,
        session: Session,
        susie_output: dict[str, object],
        variant_index: DataFrame,
    ) -> None:
        """Both `locus.standardError` and the top-level column are populated."""
        import numpy as np

        ld_matrix = np.full((self.P_VARIANTS, self.P_VARIANTS), 0.9)
        np.fill_diagonal(ld_matrix, 1.0)

        study_locus = SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            studyId="BigBrain_eqtl_EUR_ENSG00000000001",
            region="1:1-2000",
            variant_index=variant_index,
            ld_matrix=ld_matrix,
            locusStart=1,
            locusEnd=2000,
        )
        assert study_locus is not None

        row = study_locus.df.collect()[0]
        locus_errors = {tag["variantId"]: tag["standardError"] for tag in row["locus"]}
        assert locus_errors, "credible set carried no locus entries"
        assert all(value is not None for value in locus_errors.values()), (
            f"null standardError in locus: {locus_errors}"
        )

        expected = dict(
            zip(
                [f"1_{1000 + i}_A_G" for i in range(self.P_VARIANTS)],
                self.STANDARD_ERRORS,
            )
        )
        for variant_id, value in locus_errors.items():
            assert value == pytest.approx(expected[variant_id])

        # The top-level value is the lead variant's, matching how `beta` is set.
        assert row["standardError"] == pytest.approx(expected[row["variantId"]])
