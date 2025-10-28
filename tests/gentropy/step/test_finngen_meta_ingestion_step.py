"""Test FinnGen UKBB MVP meta ingestion step."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.finngen_ukb_mvp_meta import (
    FinngenUkbMvpMetaSummaryStatisticsIngestionStep,
)


class TestFinnGenMetaIngestionStep:
    """Test suite for FinnGen UKBB MVP meta ingestion step."""

    @pytest.fixture
    def df(self, session: Session) -> DataFrame:
        """Fixture for a simple DataFrame, used for mocking IO ops."""
        return session.spark.createDataFrame(
            [Row(A=1, B="a"), Row(A=2, B="b")], schema="A INT, B STRING"
        )

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.EFOMapping")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaStudyIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantDirection")
    @patch("pyspark.sql.readwriter.DataFrameReader.parquet")
    @patch("gentropy.finngen_ukb_mvp_meta.StudyIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatistics")
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatisticsQC")
    def test_step(
        self,
        qc_mock: MagicMock,
        hss_mock: MagicMock,
        si_mock: MagicMock,
        spark_read_parquet_mock: MagicMock,
        vd_mock: MagicMock,
        vi_mock: MagicMock,
        fss_mock: MagicMock,
        fsi_mock: MagicMock,
        efo_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        df: DataFrame,
    ) -> None:
        """Test step execution using mocks."""
        # Any dataset mock
        dataset = MagicMock()
        dataset.df = df
        paths = ["path1", "path2", "path3"]
        dataset.get_summary_statistics_paths = MagicMock(return_value=paths)
        dataset.annotate_sumstats_qc = MagicMock(return_value=dataset)

        # Assertion setup for building finngen manifest from path
        manifest_mock.from_path = MagicMock()

        # Assertion setup for building EFO mapping from path
        efo_mock.from_path = MagicMock()

        # Assertion setup for building finngen study index from manifest and annotating with sumstats QC
        fsi_mock.from_finngen_manifest = MagicMock(return_value=dataset)

        # Assertion setup for downloading summary statistics
        fss_mock.bgzip_to_parquet = MagicMock()
        fss_mock.N_THREAD_OPTIMAL = 10  # Example optimal thread count for testing

        # Assertion setup for variant index from parquet
        vi_mock.from_parquet = MagicMock()

        # Assertion setup for variant direction from variant index
        vd_mock.from_variant_index = MagicMock()

        # Setup mock for Spark parquet reader to return our test dataframe
        spark_read_parquet_mock.return_value = df

        # Setup mock for the harmonised summary statistics processing
        fss_mock.from_source = MagicMock(return_value=dataset)

        si_mock.from_parquet = MagicMock(return_value=dataset)

        hss_mock.from_parquet = MagicMock(return_value=dataset)

        # Setup mock for building QC from summary statistics
        qc_mock.from_summary_statistics = MagicMock(return_value=dataset)

        # Set up paths to the mock
        source_manifest_path = (tmp_path / "source_manifest").as_posix()
        efo_curation_path = (tmp_path / "efo_curation").as_posix()
        gnomad_variant_index_path = (tmp_path / "gnomad_variant_index").as_posix()
        study_index_output_path = (tmp_path / "study_index_output").as_posix()
        raw_summary_statistics_output_path = (
            tmp_path / "raw_summary_stats_output"
        ).as_posix()
        harmonised_summary_statistics_output_path = (
            tmp_path / "harmonised_summary_stats_output"
        ).as_posix()
        harmonised_summary_statistics_qc_output_path = (
            tmp_path / "harmonised_summary_stats_qc_output"
        ).as_posix()

        # Run the step
        FinngenUkbMvpMetaSummaryStatisticsIngestionStep(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            gnomad_variant_index_path=gnomad_variant_index_path,
            study_index_output_path=study_index_output_path,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            harmonised_summary_statistics_output_path=harmonised_summary_statistics_output_path,
            harmonised_summary_statistics_qc_output_path=harmonised_summary_statistics_qc_output_path,
            perform_meta_analysis_filter=True,
            imputation_score_threshold=0.8,
            perform_imputation_score_filter=True,
            perform_min_allele_count_filter=True,
            min_allele_count_threshold=20,
            perform_min_allele_frequency_filter=True,
            min_allele_frequency_threshold=0.01,
            filter_out_ambiguous_variants=True,
            qc_threshold=1e-8,
        )

        # Assertions to ensure the mocks were called as expected
        manifest_mock.from_path.assert_called_once_with(
            session=session, manifest_path=source_manifest_path
        )

        efo_mock.from_path.assert_called_once_with(
            session=session, efo_curation_path=efo_curation_path
        )

        fsi_mock.from_finngen_manifest.assert_called_once_with(
            manifest=manifest_mock.from_path.return_value,
            efo_mapping=efo_mock.from_path.return_value,
        )

        fss_mock.bgzip_to_parquet.assert_called_once_with(
            session=session,
            summary_statistics_list=paths,
            datasource=manifest_mock.from_path.return_value.meta,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            n_threads=fss_mock.N_THREAD_OPTIMAL,
        )

        vi_mock.from_parquet.assert_called_once_with(
            session=session, path=gnomad_variant_index_path
        )

        vd_mock.from_variant_index.assert_called_once_with(
            variant_index=vi_mock.from_parquet.return_value
        )

        # Verify that session.spark.read.parquet was called with the correct path
        spark_read_parquet_mock.assert_called_once_with(
            raw_summary_statistics_output_path
        )

        fss_mock.from_source.assert_called_once_with(
            raw_summary_statistics=spark_read_parquet_mock.return_value,
            finngen_manifest=manifest_mock.from_path.return_value,
            variant_annotations=vd_mock.from_variant_index.return_value,
            perform_meta_analysis_filter=True,
            imputation_score_threshold=0.8,
            perform_imputation_score_filter=True,
            min_allele_count_threshold=20,
            perform_min_allele_count_filter=True,
            min_allele_frequency_threshold=0.01,
            perform_min_allele_frequency_filter=True,
            filter_out_ambiguous_variants=True,
        )

        hss_mock.from_parquet.assert_called_once_with(
            session=session,
            path=harmonised_summary_statistics_output_path,
        )

        qc_mock.from_summary_statistics.assert_called_once_with(
            gwas=hss_mock.from_parquet.return_value, pval_threshold=1e-8
        )

        si_mock.from_parquet.assert_called_once_with(
            session=session,
            path=study_index_output_path,
        )

        si_mock.from_parquet.return_value.annotate_sumstats_qc.assert_called_once_with(
            qc_mock.from_summary_statistics.return_value
        )
