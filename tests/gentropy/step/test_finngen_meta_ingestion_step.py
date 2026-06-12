"""Test FinnGen two-way and three-way meta-analysis ingestion steps."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.datasource.finngen_meta import MetaAnalysisHarmonisationConfig
from gentropy.finngen_meta import (
    FinngenMetaStudyIndexQCAnnotationStep,
    FinngenMetaStudyIndexStep,
    ThreeWayMetaSumstatConversionStep,
    ThreeWayMetaSumstatHarmonisationStep,
    TwoWayMetaSumstatConversionStep,
    TwoWayMetaSumstatHarmonisationStep,
)


@pytest.fixture
def mock_df(session: Session) -> DataFrame:
    """Minimal DataFrame used as a stand-in for any Spark dataset.

    Includes ``studyId``, ``chromosome``, and ``position`` so writes with
    ``partitionBy("studyId", "chromosome")`` and
    ``repartitionByRange(..., "position")`` both succeed.
    """
    return session.spark.createDataFrame(
        [
            Row(studyId="study1", chromosome="1", position=100, A=1, B="a"),
            Row(studyId="study2", chromosome="2", position=200, A=2, B="b"),
        ],
        schema="studyId STRING, chromosome STRING, position INT, A INT, B STRING",
    )


@pytest.fixture
def mock_dataset(mock_df: DataFrame) -> MagicMock:
    """Generic dataset mock backed by a real DataFrame so Spark writes succeed."""
    dataset = MagicMock()
    dataset.df = mock_df
    return dataset


# ---------------------------------------------------------------------------
# SumstatHarmonisationConfig – validation unit tests
# ---------------------------------------------------------------------------


class TestSumstatHarmonisationConfig:
    """Unit tests for the Pydantic harmonisation config model."""

    def test_default_values_are_valid(self) -> None:
        """Default configuration should pass validation without errors."""
        config = MetaAnalysisHarmonisationConfig()
        assert config.imputation_score_threshold == 0.8
        assert config.min_allele_count_threshold == 20
        assert config.min_allele_frequency_threshold == 1e-4

    def test_imputation_score_above_one_raises(self) -> None:
        """imputation_score_threshold must be <= 1.0."""
        with pytest.raises(ValidationError):
            MetaAnalysisHarmonisationConfig(imputation_score_threshold=1.5)

    def test_imputation_score_below_zero_raises(self) -> None:
        """imputation_score_threshold must be >= 0.0."""
        with pytest.raises(ValidationError):
            MetaAnalysisHarmonisationConfig(imputation_score_threshold=-0.1)

    def test_min_allele_count_zero_raises(self) -> None:
        """min_allele_count_threshold must be >= 1."""
        with pytest.raises(ValidationError):
            MetaAnalysisHarmonisationConfig(min_allele_count_threshold=0)

    def test_min_allele_frequency_zero_raises(self) -> None:
        """min_allele_frequency_threshold must be > 0.0."""
        with pytest.raises(ValidationError):
            MetaAnalysisHarmonisationConfig(min_allele_frequency_threshold=0.0)

    def test_min_allele_frequency_half_raises(self) -> None:
        """min_allele_frequency_threshold must be < 0.5."""
        with pytest.raises(ValidationError):
            MetaAnalysisHarmonisationConfig(min_allele_frequency_threshold=0.5)

    def test_model_dump_keys_match_harmonisation_config_fields(self) -> None:
        """model_dump() must expose exactly the fields used to build the config."""
        expected_keys = {
            "perform_meta_analysis_filter",
            "perform_imputation_score_filter",
            "imputation_score_threshold",
            "perform_min_allele_count_filter",
            "min_allele_count_threshold",
            "perform_min_allele_frequency_filter",
            "min_allele_frequency_threshold",
            "perform_samples_size_filter",
            "sample_size_threshold",
            "flipping_window_size",
            "remove_monomorphic_alleles",
            "remove_ambiguous_alleles",
            "verify_atgc",
        }
        assert (
            set(MetaAnalysisHarmonisationConfig().model_dump().keys()) == expected_keys
        )


# ---------------------------------------------------------------------------
# Step 1: FinngenMetaStudyIndexStep
# ---------------------------------------------------------------------------


class TestFinngenMetaStudyIndexStep:
    """Tests for step 1 – study index creation."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.EFOMapping")
    @patch("gentropy.finngen_meta.FinnGenMetaManifest")
    def test_step(
        self,
        manifest_mock: MagicMock,
        efo_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_dataset: MagicMock,
    ) -> None:
        """Step builds the study index via FinnGenMetaManifest.from_source and writes it."""
        manifest_mock.from_source.return_value = mock_dataset
        manifest_path = (tmp_path / "manifest").as_posix()
        efo_curation_path = (tmp_path / "efo").as_posix()
        study_index_output_path = (tmp_path / "study_index").as_posix()

        FinngenMetaStudyIndexStep(
            session=session,
            manifest_path=manifest_path,
            efo_curation_path=efo_curation_path,
            study_index_output_path=study_index_output_path,
            finngen_release="R12",
        )

        efo_mock.from_path.assert_called_once_with(
            session=session, efo_curation_path=efo_curation_path
        )
        manifest_mock.from_source.assert_called_once()
        call_kwargs = manifest_mock.from_source.call_args.kwargs
        assert call_kwargs["manifest_path"] == manifest_path
        assert call_kwargs["efo_mapping"] == efo_mock.from_path.return_value

    @pytest.mark.step_test
    def test_step_raises_for_unsupported_release(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Step raises NotImplementedError for releases other than R12."""
        with pytest.raises(NotImplementedError):
            FinngenMetaStudyIndexStep(
                session=session,
                manifest_path=(tmp_path / "manifest").as_posix(),
                efo_curation_path=(tmp_path / "efo").as_posix(),
                study_index_output_path=(tmp_path / "out").as_posix(),
                finngen_release="R11",
            )


# ---------------------------------------------------------------------------
# Step 2: ThreeWayMetaSumstatConversionStep
# ---------------------------------------------------------------------------


class TestThreeWayMetaSumstatConversionStep:
    """Tests for step 2 – BGZIP → Parquet conversion."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.ThreeWaySummaryStatistics")
    def test_step(
        self,
        fss_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step lists paths from the glob and calls bgzip_to_parquet."""
        paths = [f"path{i}" for i in range(5)]
        session.list_hadoop_paths = MagicMock(return_value=paths)  # type: ignore[method-assign]
        fss_mock.bgzip_to_parquet = MagicMock()
        fss_mock.N_THREAD_OPTIMAL = 10

        glob = "gs://bucket/*.tsv.gz"
        raw_output_path = (tmp_path / "raw_sumstats").as_posix()

        ThreeWayMetaSumstatConversionStep(
            session=session,
            summary_statistics_glob=glob,
            raw_summary_statistics_output_path=raw_output_path,
            finngen_release="R12",
        )

        session.list_hadoop_paths.assert_called_once_with(glob)
        fss_mock.bgzip_to_parquet.assert_called_once_with(
            session=session,
            summary_statistics_list=paths,
            raw_summary_statistics_output_path=raw_output_path,
            n_threads=fss_mock.N_THREAD_OPTIMAL,
            meta_analysis_type=fss_mock.bgzip_to_parquet.call_args.kwargs[
                "meta_analysis_type"
            ],
            finngen_release=fss_mock.bgzip_to_parquet.call_args.kwargs[
                "finngen_release"
            ],
        )

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.ThreeWaySummaryStatistics")
    def test_step_raises_when_too_few_paths(
        self,
        fss_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step raises AssertionError when the glob resolves to a single path or less."""
        session.list_hadoop_paths = MagicMock(return_value=["only_one_path"])  # type: ignore[method-assign]

        with pytest.raises(AssertionError):
            ThreeWayMetaSumstatConversionStep(
                session=session,
                summary_statistics_glob="gs://bucket/single.tsv.gz",
                raw_summary_statistics_output_path=(tmp_path / "raw").as_posix(),
            )


# ---------------------------------------------------------------------------
# Step 3: ThreeWayMetaSumstatHarmonisationStep
# ---------------------------------------------------------------------------


class TestThreeWayHarmonisationStep:
    """Tests for step 3 – summary statistics harmonisation."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.VariantDirection")
    @patch("gentropy.finngen_meta.MetaAnalysisStudyIndex")
    @patch("pyspark.sql.readwriter.DataFrameReader.parquet")
    @patch("gentropy.finngen_meta.ThreeWaySummaryStatistics")
    def test_step(
        self,
        fss_mock: MagicMock,
        spark_read_parquet_mock: MagicMock,
        msi_mock: MagicMock,
        vd_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_df: DataFrame,
        mock_dataset: MagicMock,
    ) -> None:
        """Step reads inputs, harmonises and writes partitioned by studyId and chromosome."""
        spark_read_parquet_mock.return_value = mock_df
        fss_mock.from_source.return_value = mock_dataset

        meta_index_path = (tmp_path / "meta_index").as_posix()
        vd_path = (tmp_path / "variant_direction").as_posix()
        raw_path = (tmp_path / "raw").as_posix()
        harmonised_path = (tmp_path / "harmonised").as_posix()

        ThreeWayMetaSumstatHarmonisationStep(
            session=session,
            meta_analysis_study_index_path=meta_index_path,
            variant_direction_path=vd_path,
            raw_summary_statistics_output_path=raw_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            perform_meta_analysis_filter=True,
            imputation_score_threshold=0.8,
            perform_imputation_score_filter=True,
            min_allele_count_threshold=20,
            perform_min_allele_count_filter=True,
            min_allele_frequency_threshold=1e-4,
            perform_min_allele_frequency_filter=False,
            perform_samples_size_filter=True,
            sample_size_threshold=1000,
            remove_ambiguous_alleles=False,
            verify_atgc=True,
            remove_monomorphic_alleles=True,
        )

        msi_mock.from_parquet.assert_called_once_with(
            session=session, path=meta_index_path
        )
        vd_mock.from_parquet.assert_called_once_with(session=session, path=vd_path)
        spark_read_parquet_mock.assert_called_once_with(raw_path)
        fss_mock.from_source.assert_called_once()

    @pytest.mark.step_test
    def test_step_raises_on_invalid_config(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Step raises ValidationError when harmonisation config values are out of range."""
        with pytest.raises(ValidationError):
            ThreeWayMetaSumstatHarmonisationStep(
                session=session,
                meta_analysis_study_index_path=(tmp_path / "meta_index").as_posix(),
                variant_direction_path=(tmp_path / "vd").as_posix(),
                raw_summary_statistics_output_path=(tmp_path / "raw").as_posix(),
                harmonised_summary_statistics_output_path=(
                    tmp_path / "harmonised"
                ).as_posix(),
                perform_meta_analysis_filter=True,
                imputation_score_threshold=2.0,
                perform_imputation_score_filter=True,
                min_allele_count_threshold=20,
                perform_min_allele_count_filter=True,
                min_allele_frequency_threshold=1e-4,
                perform_min_allele_frequency_filter=False,
                perform_samples_size_filter=True,
                sample_size_threshold=1000,
                remove_ambiguous_alleles=False,
                verify_atgc=True,
                remove_monomorphic_alleles=True,
            )


# ---------------------------------------------------------------------------
# Two-way: conversion and harmonisation steps
# ---------------------------------------------------------------------------


class TestTwoWayMetaSumstatConversionStep:
    """Tests for the two-way BGZIP → Parquet conversion step."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.TwoWaySummaryStatistics")
    def test_step(
        self,
        fss_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step lists paths from the glob and calls bgzip_to_parquet."""
        paths = [f"path{i}" for i in range(5)]
        session.list_hadoop_paths = MagicMock(return_value=paths)  # type: ignore[method-assign]
        fss_mock.bgzip_to_parquet = MagicMock()

        glob = "gs://bucket/*.tsv.gz"
        raw_output_path = (tmp_path / "raw_sumstats").as_posix()

        TwoWayMetaSumstatConversionStep(
            session=session,
            summary_statistics_glob=glob,
            raw_summary_statistics_output_path=raw_output_path,
            finngen_release="R12",
        )

        session.list_hadoop_paths.assert_called_once_with(glob)
        fss_mock.bgzip_to_parquet.assert_called_once()
        kwargs = fss_mock.bgzip_to_parquet.call_args.kwargs
        assert kwargs["summary_statistics_list"] == paths
        assert kwargs["raw_summary_statistics_output_path"] == raw_output_path

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.TwoWaySummaryStatistics")
    def test_step_raises_when_too_few_paths(
        self,
        fss_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step raises AssertionError when the glob resolves to a single path or less."""
        session.list_hadoop_paths = MagicMock(return_value=["only_one_path"])  # type: ignore[method-assign]

        with pytest.raises(AssertionError):
            TwoWayMetaSumstatConversionStep(
                session=session,
                summary_statistics_glob="gs://bucket/single.tsv.gz",
                raw_summary_statistics_output_path=(tmp_path / "raw").as_posix(),
            )


class TestTwoWayHarmonisationStep:
    """Tests for the two-way summary statistics harmonisation step."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.VariantDirection")
    @patch("gentropy.finngen_meta.MetaAnalysisStudyIndex")
    @patch("pyspark.sql.readwriter.DataFrameReader.parquet")
    @patch("gentropy.finngen_meta.TwoWaySummaryStatistics")
    def test_step(
        self,
        fss_mock: MagicMock,
        spark_read_parquet_mock: MagicMock,
        msi_mock: MagicMock,
        vd_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        mock_df: DataFrame,
        mock_dataset: MagicMock,
    ) -> None:
        """Step reads inputs, harmonises and writes partitioned by studyId and chromosome."""
        spark_read_parquet_mock.return_value = mock_df
        fss_mock.from_source.return_value = mock_dataset

        meta_index_path = (tmp_path / "meta_index").as_posix()
        vd_path = (tmp_path / "variant_direction").as_posix()
        raw_path = (tmp_path / "raw").as_posix()
        harmonised_path = (tmp_path / "harmonised").as_posix()

        TwoWayMetaSumstatHarmonisationStep(
            session=session,
            meta_analysis_study_index_path=meta_index_path,
            variant_direction_path=vd_path,
            raw_summary_statistics_output_path=raw_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            perform_meta_analysis_filter=True,
            min_allele_count_threshold=20,
            perform_min_allele_count_filter=True,
            min_allele_frequency_threshold=1e-4,
            perform_min_allele_frequency_filter=False,
            perform_samples_size_filter=True,
            sample_size_threshold=1000,
            remove_ambiguous_alleles=False,
            verify_atgc=True,
            remove_monomorphic_alleles=True,
        )

        msi_mock.from_parquet.assert_called_once_with(
            session=session, path=meta_index_path
        )
        vd_mock.from_parquet.assert_called_once_with(session=session, path=vd_path)
        spark_read_parquet_mock.assert_called_once_with(raw_path)
        fss_mock.from_source.assert_called_once()
        # studyId must come from the Parquet partition column, not finngen_release.
        assert "finngen_release" not in fss_mock.from_source.call_args.kwargs


# ---------------------------------------------------------------------------
# Step 4: FinngenMetaStudyIndexQCAnnotationStep
# ---------------------------------------------------------------------------


class TestFinngenMetaStudyIndexQCAnnotationStep:
    """Tests for step 4 – QC computation and study index annotation."""

    @pytest.mark.step_test
    @patch("gentropy.finngen_meta.SummaryStatistics")
    @patch("gentropy.finngen_meta.SummaryStatisticsQC")
    @patch("gentropy.finngen_meta.MetaAnalysisStudyIndex")
    def test_step(
        self,
        msi_mock: MagicMock,
        qc_mock: MagicMock,
        hss_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Step computes QC, annotates the study index, and writes both outputs."""
        study_index_path = (tmp_path / "study_index").as_posix()
        harmonised_path = (tmp_path / "harmonised").as_posix()
        qc_output_path = (tmp_path / "qc").as_posix()
        si_with_qc_path = (tmp_path / "study_index_with_qc").as_posix()

        FinngenMetaStudyIndexQCAnnotationStep(
            session=session,
            study_index_output_path=study_index_path,
            harmonised_summary_statistics_output_path=harmonised_path,
            harmonised_summary_statistics_qc_output_path=qc_output_path,
            study_index_with_qc_output_path=si_with_qc_path,
            qc_threshold=1e-8,
        )

        hss_mock.from_parquet.assert_called_once_with(
            session=session, path=harmonised_path
        )
        qc_mock.from_summary_statistics.assert_called_once_with(
            gwas=hss_mock.from_parquet.return_value, pval_threshold=1e-8
        )
        msi_mock.from_parquet.assert_called_once_with(
            session=session, path=study_index_path
        )
        msi_mock.from_parquet.return_value.to_study.assert_called_once()

    def test_step_raises_on_invalid_qc_threshold(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Step raises AssertionError when qc_threshold >= 1.0."""
        with pytest.raises(AssertionError, match="less than 1.0"):
            FinngenMetaStudyIndexQCAnnotationStep(
                session=session,
                study_index_output_path=(tmp_path / "study_index").as_posix(),
                harmonised_summary_statistics_output_path=(
                    tmp_path / "harmonised"
                ).as_posix(),
                harmonised_summary_statistics_qc_output_path=(
                    tmp_path / "qc"
                ).as_posix(),
                study_index_with_qc_output_path=(tmp_path / "out").as_posix(),
                qc_threshold=1.5,
            )
