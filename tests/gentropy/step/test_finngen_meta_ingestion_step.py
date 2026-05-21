"""Test FinnGen UKBB MVP meta ingestion steps."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.finngen_ukb_mvp_meta import (
    FinngenUkbMvpMetaStudyIndexQCAnnotationStep,
    FinngenUkbMvpMetaStudyIndexStep,
    FinngenUkbMvpMetaSummaryStatisticsIngestionStep,
    FinngenUkbMvpMetaSumstatConversionStep,
    FinngenUkbMvpMetaSumstatHarmonisationStep,
    _SumstatHarmonisationConfig,
)


@pytest.fixture
def mock_df(session: Session) -> DataFrame:
    """Minimal DataFrame used as a stand-in for any Spark dataset.

    Includes ``studyId`` so writes with ``partitionBy("studyId")`` succeed.
    """
    return session.spark.createDataFrame(
        [Row(studyId="study1", A=1, B="a"), Row(studyId="study2", A=2, B="b")],
        schema="studyId STRING, A INT, B STRING",
    )


@pytest.fixture
def mock_dataset(mock_df: DataFrame) -> MagicMock:
    """Generic dataset mock backed by a real DataFrame so Spark writes succeed."""
    dataset = MagicMock()
    dataset.df = mock_df
    dataset.get_summary_statistics_paths = MagicMock(
        return_value=["path1", "path2", "path3"]
    )
    dataset.annotate_sumstats_qc = MagicMock(return_value=MagicMock())
    return dataset


# ---------------------------------------------------------------------------
# _SumstatHarmonisationConfig – validation unit tests
# ---------------------------------------------------------------------------


class TestSumstatHarmonisationConfig:
    """Unit tests for the Pydantic harmonisation config model."""

    def test_default_values_are_valid(self) -> None:
        """Default configuration should pass validation without errors."""
        config = _SumstatHarmonisationConfig()
        assert config.imputation_score_threshold == 0.8
        assert config.min_allele_count_threshold == 20
        assert config.min_allele_frequency_threshold == 1e-4

    def test_imputation_score_above_one_raises(self) -> None:
        """imputation_score_threshold must be <= 1.0."""
        with pytest.raises(ValidationError):
            _SumstatHarmonisationConfig(imputation_score_threshold=1.5)

    def test_imputation_score_below_zero_raises(self) -> None:
        """imputation_score_threshold must be >= 0.0."""
        with pytest.raises(ValidationError):
            _SumstatHarmonisationConfig(imputation_score_threshold=-0.1)

    def test_min_allele_count_zero_raises(self) -> None:
        """min_allele_count_threshold must be >= 1."""
        with pytest.raises(ValidationError):
            _SumstatHarmonisationConfig(min_allele_count_threshold=0)

    def test_min_allele_frequency_zero_raises(self) -> None:
        """min_allele_frequency_threshold must be > 0.0."""
        with pytest.raises(ValidationError):
            _SumstatHarmonisationConfig(min_allele_frequency_threshold=0.0)

    def test_min_allele_frequency_one_raises(self) -> None:
        """min_allele_frequency_threshold must be < 1.0."""
        with pytest.raises(ValidationError):
            _SumstatHarmonisationConfig(min_allele_frequency_threshold=1.0)

    def test_model_dump_keys_match_from_source_signature(self) -> None:
        """model_dump() keys must match the kwargs accepted by FinnGenUkbMvpMetaSummaryStatistics.from_source."""
        expected_keys = {
            "perform_meta_analysis_filter",
            "imputation_score_threshold",
            "perform_imputation_score_filter",
            "min_allele_count_threshold",
            "perform_min_allele_count_filter",
            "min_allele_frequency_threshold",
            "perform_min_allele_frequency_filter",
            "filter_out_ambiguous_variants",
        }
        assert set(_SumstatHarmonisationConfig().model_dump().keys()) == expected_keys


# ---------------------------------------------------------------------------
# Step 1: FinngenUkbMvpMetaStudyIndexStep
# ---------------------------------------------------------------------------


class TestFinngenUkbMvpMetaStudyIndexStep:
    """Tests for step 1 – study index creation."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.EFOMapping")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaStudyIndex")
    def test_step(
        self,
        fsi_mock: MagicMock,
        efo_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_dataset: MagicMock,
    ) -> None:
        """Step builds the study index and writes it to the output path."""
        fsi_mock.from_finngen_manifest.return_value = mock_dataset
        source_manifest_path = (tmp_path / "manifest").as_posix()
        efo_curation_path = (tmp_path / "efo").as_posix()
        study_index_output_path = (tmp_path / "study_index").as_posix()

        FinngenUkbMvpMetaStudyIndexStep(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            study_index_output_path=study_index_output_path,
            finngen_release="R12",
        )

        manifest_mock.from_path.assert_called_once_with(
            session=session, manifest_path=source_manifest_path
        )
        efo_mock.from_path.assert_called_once_with(
            session=session, efo_curation_path=efo_curation_path
        )
        fsi_mock.from_finngen_manifest.assert_called_once_with(
            manifest=manifest_mock.from_path.return_value,
            efo_mapping=efo_mock.from_path.return_value,
            finngen_release="R12",
        )


# ---------------------------------------------------------------------------
# Step 2: FinngenUkbMvpMetaSumstatConversionStep
# ---------------------------------------------------------------------------


class TestFinngenUkbMvpMetaSumstatConversionStep:
    """Tests for step 2 – BGZIP → Parquet conversion."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.StudyIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    def test_step(
        self,
        fss_mock: MagicMock,
        si_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_dataset: MagicMock,
    ) -> None:
        """Step reads paths from study index and calls bgzip_to_parquet."""
        paths = ["path1", "path2", "path3"]
        si_mock.from_parquet.return_value = mock_dataset
        mock_dataset.get_summary_statistics_paths.return_value = paths
        fss_mock.bgzip_to_parquet = MagicMock()
        fss_mock.N_THREAD_OPTIMAL = 4

        source_manifest_path = (tmp_path / "manifest").as_posix()
        study_index_output_path = (tmp_path / "study_index").as_posix()
        raw_output_path = (tmp_path / "raw_sumstats").as_posix()

        FinngenUkbMvpMetaSumstatConversionStep(
            session=session,
            source_manifest_path=source_manifest_path,
            study_index_output_path=study_index_output_path,
            raw_summary_statistics_output_path=raw_output_path,
        )

        manifest_mock.from_path.assert_called_once_with(
            session=session, manifest_path=source_manifest_path
        )
        si_mock.from_parquet.assert_called_once_with(
            session=session, path=study_index_output_path
        )
        fss_mock.bgzip_to_parquet.assert_called_once_with(
            session=session,
            summary_statistics_list=paths,
            datasource=manifest_mock.from_path.return_value.meta,
            raw_summary_statistics_output_path=raw_output_path,
            n_threads=fss_mock.N_THREAD_OPTIMAL,
        )

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.StudyIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    def test_step_raises_when_no_paths(
        self,
        fss_mock: MagicMock,
        si_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_dataset: MagicMock,
    ) -> None:
        """Step raises AssertionError when study index contains no summary statistics paths."""
        mock_dataset.get_summary_statistics_paths.return_value = []
        si_mock.from_parquet.return_value = mock_dataset

        with pytest.raises(AssertionError, match="No summary statistics paths"):
            FinngenUkbMvpMetaSumstatConversionStep(
                session=session,
                source_manifest_path=(tmp_path / "manifest").as_posix(),
                study_index_output_path=(tmp_path / "study_index").as_posix(),
                raw_summary_statistics_output_path=(tmp_path / "raw").as_posix(),
            )


# ---------------------------------------------------------------------------
# Step 3: FinngenUkbMvpMetaSumstatHarmonisationStep
# ---------------------------------------------------------------------------


class TestFinngenUkbMvpMetaSumstatHarmonisationStep:
    """Tests for step 3 – summary statistics harmonisation."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantDirection")
    @patch("pyspark.sql.readwriter.DataFrameReader.parquet")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    def test_step(
        self,
        fss_mock: MagicMock,
        spark_read_parquet_mock: MagicMock,
        vd_mock: MagicMock,
        vi_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_df: DataFrame,
        mock_dataset: MagicMock,
    ) -> None:
        """Step harmonises raw sumstats and writes output partitioned by studyId."""
        spark_read_parquet_mock.return_value = mock_df
        fss_mock.from_source.return_value = mock_dataset

        source_manifest_path = (tmp_path / "manifest").as_posix()
        gnomad_path = (tmp_path / "gnomad").as_posix()
        raw_path = (tmp_path / "raw").as_posix()
        harmonised_path = (tmp_path / "harmonised").as_posix()

        FinngenUkbMvpMetaSumstatHarmonisationStep(
            session=session,
            source_manifest_path=source_manifest_path,
            gnomad_variant_index_path=gnomad_path,
            raw_summary_statistics_output_path=raw_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            imputation_score_threshold=0.8,
            min_allele_count_threshold=20,
            min_allele_frequency_threshold=1e-4,
        )

        manifest_mock.from_path.assert_called_once_with(
            session=session, manifest_path=source_manifest_path
        )
        vi_mock.from_parquet.assert_called_once_with(session=session, path=gnomad_path)
        vd_mock.from_variant_index.assert_called_once_with(
            variant_index=vi_mock.from_parquet.return_value
        )
        spark_read_parquet_mock.assert_called_once_with(raw_path)
        fss_mock.from_source.assert_called_once_with(
            raw_summary_statistics=mock_df,
            finngen_manifest=manifest_mock.from_path.return_value,
            variant_annotations=vd_mock.from_variant_index.return_value,
            perform_meta_analysis_filter=True,
            imputation_score_threshold=0.8,
            perform_imputation_score_filter=True,
            min_allele_count_threshold=20,
            perform_min_allele_count_filter=True,
            min_allele_frequency_threshold=1e-4,
            perform_min_allele_frequency_filter=False,
            filter_out_ambiguous_variants=False,
        )

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantDirection")
    @patch("pyspark.sql.readwriter.DataFrameReader.parquet")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    def test_step_raises_on_invalid_config(
        self,
        fss_mock: MagicMock,
        spark_read_parquet_mock: MagicMock,
        vd_mock: MagicMock,
        vi_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step raises ValidationError when harmonisation config values are out of range."""
        with pytest.raises(ValidationError):
            FinngenUkbMvpMetaSumstatHarmonisationStep(
                session=session,
                source_manifest_path=(tmp_path / "manifest").as_posix(),
                gnomad_variant_index_path=(tmp_path / "gnomad").as_posix(),
                raw_summary_statistics_output_path=(tmp_path / "raw").as_posix(),
                harmonised_summary_statistics_output_path=(
                    tmp_path / "harmonised"
                ).as_posix(),
                imputation_score_threshold=2.0,  # invalid: > 1.0
            )


# ---------------------------------------------------------------------------
# Step 4: FinngenUkbMvpMetaStudyIndexQCAnnotationStep
# ---------------------------------------------------------------------------


class TestFinngenUkbMvpMetaStudyIndexQCAnnotationStep:
    """Tests for step 4 – QC computation and study index annotation."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatistics")
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatisticsQC")
    @patch("gentropy.finngen_ukb_mvp_meta.StudyIndex")
    def test_step(
        self,
        si_mock: MagicMock,
        qc_mock: MagicMock,
        hss_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step computes QC, writes results, and annotates the study index."""
        study_index_path = (tmp_path / "study_index").as_posix()
        harmonised_path = (tmp_path / "harmonised").as_posix()
        qc_output_path = (tmp_path / "qc").as_posix()

        FinngenUkbMvpMetaStudyIndexQCAnnotationStep(
            session=session,
            study_index_output_path=study_index_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            harmonised_summary_statistics_qc_output_path=qc_output_path,
            qc_threshold=1e-8,
        )

        hss_mock.from_parquet.assert_called_once_with(
            session=session, path=harmonised_path
        )
        qc_mock.from_summary_statistics.assert_called_once_with(
            gwas=hss_mock.from_parquet.return_value, pval_threshold=1e-8
        )
        si_mock.from_parquet.assert_called_once_with(
            session=session, path=study_index_path
        )
        si_mock.from_parquet.return_value.annotate_sumstats_qc.assert_called_once_with(
            qc_mock.from_summary_statistics.return_value
        )

    def test_step_raises_on_invalid_qc_threshold(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Step raises AssertionError when qc_threshold >= 1.0."""
        with pytest.raises(AssertionError, match="p-value less than 1.0"):
            FinngenUkbMvpMetaStudyIndexQCAnnotationStep(
                session=session,
                study_index_output_path=(tmp_path / "study_index").as_posix(),
                harmonised_summary_statistics_output_path=(
                    tmp_path / "harmonised"
                ).as_posix(),
                harmonised_summary_statistics_qc_output_path=(
                    tmp_path / "qc"
                ).as_posix(),
                qc_threshold=1.5,
            )


# ---------------------------------------------------------------------------
# Facade: FinngenUkbMvpMetaSummaryStatisticsIngestionStep
# ---------------------------------------------------------------------------


class TestFinngenUkbMvpMetaSummaryStatisticsIngestionStep:
    """Tests for the convenience façade that chains all four steps."""

    @pytest.mark.step_test
    @patch(
        "gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaStudyIndexStep",
        autospec=True,
    )
    @patch(
        "gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaSumstatConversionStep",
        autospec=True,
    )
    @patch(
        "gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaSumstatHarmonisationStep",
        autospec=True,
    )
    @patch(
        "gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaStudyIndexQCAnnotationStep",
        autospec=True,
    )
    def test_facade_delegates_to_all_four_steps(
        self,
        qc_step_mock: MagicMock,
        harmonisation_step_mock: MagicMock,
        conversion_step_mock: MagicMock,
        study_index_step_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Façade instantiates each sub-step exactly once with the correct arguments."""
        source_manifest_path = (tmp_path / "manifest").as_posix()
        efo_curation_path = (tmp_path / "efo").as_posix()
        gnomad_path = (tmp_path / "gnomad").as_posix()
        study_index_path = (tmp_path / "study_index").as_posix()
        raw_path = (tmp_path / "raw").as_posix()
        harmonised_path = (tmp_path / "harmonised").as_posix()
        qc_path = (tmp_path / "qc").as_posix()

        FinngenUkbMvpMetaSummaryStatisticsIngestionStep(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            gnomad_variant_index_path=gnomad_path,
            study_index_output_path=study_index_path,
            raw_summary_statistics_output_path=raw_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            harmonised_summary_statistics_qc_output_path=qc_path,
            imputation_score_threshold=0.9,
            min_allele_count_threshold=30,
            qc_threshold=1e-6,
        )

        study_index_step_mock.assert_called_once_with(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            study_index_output_path=study_index_path,
        )
        conversion_step_mock.assert_called_once_with(
            session=session,
            source_manifest_path=source_manifest_path,
            study_index_output_path=study_index_path,
            raw_summary_statistics_output_path=raw_path,
        )
        harmonisation_step_mock.assert_called_once_with(
            session=session,
            source_manifest_path=source_manifest_path,
            gnomad_variant_index_path=gnomad_path,
            raw_summary_statistics_output_path=raw_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            perform_meta_analysis_filter=True,
            imputation_score_threshold=0.9,
            perform_imputation_score_filter=True,
            min_allele_count_threshold=30,
            perform_min_allele_count_filter=True,
            min_allele_frequency_threshold=1e-4,
            perform_min_allele_frequency_filter=False,
            filter_out_ambiguous_variants=False,
        )
        qc_step_mock.assert_called_once_with(
            session=session,
            study_index_output_path=study_index_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            harmonised_summary_statistics_qc_output_path=qc_path,
            qc_threshold=1e-6,
        )
