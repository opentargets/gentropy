"""Tests for SusieFineMapperStep."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gentropy.common.session import Session
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_locus import StudyLocusQualityCheck
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
                "credible_set_output": [str(tmp_path / "credible_set")],
                "credible_set_variant_output": [str(tmp_path / "credible_set_variant")],
            }
        )
        manifest_path = str(tmp_path / "manifest.csv")
        manifest_data.to_csv(manifest_path, index=False)

        with (
            patch(
                "gentropy.susie_finemapper.AnnotatedStudyLocus.from_parquet"
            ) as mock_study_locus,
            patch(
                "gentropy.susie_finemapper.StudyIndex.from_parquet"
            ) as mock_study_index,
        ):
            # Mock the study locus and index
            mock_sl = MagicMock()
            mock_sl.df.collect.return_value = [MagicMock()]
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
                "credible_set_output": [str(tmp_path / "credible_set")],
                "credible_set_variant_output": [str(tmp_path / "credible_set_variant")],
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
            "susie_est_tausq",
            "run_carma",
            "run_sumstat_imputation",
            "carma_time_limit",
            "carma_tau",
            "imputed_r2_threshold",
            "ld_score_threshold",
            "ld_min_r2",
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
        assert sig.parameters["susie_est_tausq"].default is False
        assert sig.parameters["run_carma"].default is False
        assert sig.parameters["run_sumstat_imputation"].default is False
        assert sig.parameters["carma_time_limit"].default == 600
        assert sig.parameters["carma_tau"].default == 0.15
        assert sig.parameters["imputed_r2_threshold"].default == 0.9
        assert sig.parameters["ld_score_threshold"].default == 5
        assert sig.parameters["ld_min_r2"].default == 0.8

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


class TestSusieInfToCredibleSets:
    """Test the conversion of SuSiE-inf output to credible sets and their variants.

    Four variants and two effects. Effect 1 puts 0.9 on the first variant and effect 2 0.8 on the
    last, so their 95% credible sets are the first two and the last two variants. The third
    variant is not in the locus, as if it were imputed.
    """

    VARIANTS = pd.DataFrame(
        {
            "variantId": ["1_100_A_T", "1_200_C_G", "1_300_G_A", "1_400_T_C"],
            "z": [10.0, 5.0, 4.0, -6.0],
        }
    )
    SUSIE_OUTPUT = {
        "PIP": np.array([[0.9, 0.01], [0.06, 0.02], [0.03, 0.17], [0.01, 0.8]]),
        "lbf_variable": np.array([[5.0, 0.0], [2.0, 0.5], [1.0, 2.0], [0.0, 6.0]]),
        "mu": np.array([[0.02, 0.0], [0.01, 0.0], [0.0, 0.005], [0.0, -0.03]]),
        "omega": np.full((4, 2), 1e4),
        "lbf": np.array([10.0, 8.0]),
    }
    LD = np.array(
        [
            [1.0, 0.8, 0.1, 0.1],
            [0.8, 1.0, 0.1, 0.1],
            [0.1, 0.1, 1.0, 0.7],
            [0.1, 0.1, 0.7, 1.0],
        ]
    )
    MARGINALS = [
        ("1_100_A_T", 1.0, -10, 0.3, 5000, 0.2, 0.02),
        ("1_200_C_G", 2.0, -6, None, 5000, 0.1, 0.02),
        ("1_400_T_C", 3.0, -9, 0.4, 5000, -0.15, 0.025),
    ]

    def _credible_sets(
        self,
        session: Session,
        lead_pval_threshold: float | None = None,
        with_marginals: bool = True,
    ) -> tuple[CredibleSet, CredibleSetVariant] | None:
        """Convert the test SuSiE-inf output.

        Args:
            session (Session): Session object.
            lead_pval_threshold (float | None): Lead p-value threshold.
            with_marginals (bool): Whether to give the locus statistics.

        Returns:
            tuple[CredibleSet, CredibleSetVariant] | None: The credible sets and their variants.
        """
        marginals = session.spark.createDataFrame(
            self.MARGINALS,
            "variantId string, pValueMantissa float, pValueExponent int, "
            "effectAlleleFrequencyFromSource float, sampleSize int, beta double, "
            "standardError double",
        )
        return SusieFineMapperStep.susie_inf_to_credible_sets(
            susie_output=self.SUSIE_OUTPUT,
            session=session,
            studyId="S1",
            variants=self.VARIANTS,
            ld_matrix=self.LD,
            locusStart=1,
            locusEnd=1000,
            n=10_000,
            marginals=marginals if with_marginals else None,
            lead_pval_threshold=lead_pval_threshold,
        )

    def test_credible_sets(self, session: Session) -> None:
        """Each effect gives a credible set led by its highest alpha, flagged out-of-sample LD."""
        result = self._credible_sets(session)
        assert result is not None
        credible_sets = {row.leadVariantId: row for row in result[0].df.collect()}

        assert set(credible_sets) == {"1_100_A_T", "1_400_T_C"}
        first = credible_sets["1_100_A_T"]
        assert (first.studyId, first.studyType, first.fineMappingMethod) == (
            "S1",
            "gwas",
            "SuSiE-inf",
        )
        assert (first.chromosome, first.locusStart, first.locusEnd) == ("1", 1, 1000)
        assert first.logBayesFactor == pytest.approx(10.0)
        assert first.purityMeanR2 == pytest.approx(0.82)
        assert first.purityMinR2 == pytest.approx(0.64)
        assert first.confidence is None
        assert first.qualityControls == [StudyLocusQualityCheck.OUT_OF_SAMPLE_LD.value]

    def test_credible_set_variants(self, session: Session) -> None:
        """Members carry the locus statistics and SuSiE-inf's unscaled conditional statistics."""
        result = self._credible_sets(session)
        assert result is not None
        variants = {row.variantId: row for row in result[1].df.collect()}

        # The 95% cut: two members per credible set.
        assert set(variants) == {"1_100_A_T", "1_200_C_G", "1_300_G_A", "1_400_T_C"}
        lead = variants["1_100_A_T"]
        assert lead.posteriorProbability == pytest.approx(0.9)
        assert lead.logBayesFactor == pytest.approx(5.0)
        assert lead.r2Overall == pytest.approx(1.0)
        assert variants["1_200_C_G"].r2Overall == pytest.approx(0.64)
        assert (lead.pValueMantissa, lead.pValueExponent) == (1.0, -10)
        assert (lead.reportedBeta, lead.reportedStandardError) == (0.2, 0.02)
        assert lead.effectAlleleFrequency == pytest.approx(0.3)
        assert lead.sampleSize == 5000
        # mu and 1/sqrt(omega) of effect 1, unscaled:
        assert lead.conditionalBeta == pytest.approx(0.02)
        assert lead.conditionalStandardError == pytest.approx(0.01)
        assert lead.qualityControls == [
            CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value
        ]

        imputed = variants["1_300_G_A"]
        assert imputed.r2Overall == pytest.approx(0.49)
        assert imputed.pValueMantissa is None
        assert imputed.reportedBeta is None
        # SuSiE-inf's own statistics do not need the locus:
        assert imputed.conditionalBeta == pytest.approx(0.005)
        assert imputed.conditionalStandardError == pytest.approx(0.01)
        assert imputed.rescaledAbsBeta is None
        assert imputed.qualityControls == [
            CredibleSetVariantQualityCheck.NOT_IN_LOCUS.value
        ]
        assert variants["1_200_C_G"].qualityControls == []

    def test_lead_pvalue_threshold(self, session: Session) -> None:
        """The threshold drops credible sets with a weaker or unreported lead p-value."""
        result = self._credible_sets(session, lead_pval_threshold=1e-9)
        assert result is not None
        assert [row.leadVariantId for row in result[0].df.collect()] == ["1_100_A_T"]
        assert {row.variantId for row in result[1].df.collect()} == {
            "1_100_A_T",
            "1_200_C_G",
        }
        assert (
            self._credible_sets(session, lead_pval_threshold=1, with_marginals=False)
            is None
        )
