"""Test FinnGen UKBB MVP meta ingestion step."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
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

    @pytest.fixture
    def harmonised(self, session: Session) -> DataFrame:
        """Harmonised summary statistics of two studies on two chromosomes, out of position order."""
        return session.spark.createDataFrame(
            [
                (study, f"{chrom}_{pos}_A_G", chrom, pos, 0.1, 1000, 1.0, -8, 0.1, 0.01)
                for study in ["S1", "S2"]
                for chrom in ["1", "2"]
                for pos in [300, 100, 200]
            ],
            SummaryStatistics.get_schema(),
        )

    @pytest.mark.step_test
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.EFOMapping")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaStudyIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantDirection")
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatistics")
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatisticsQC")
    def test_step(
        self,
        qc_mock: MagicMock,
        hss_mock: MagicMock,
        vd_mock: MagicMock,
        vi_mock: MagicMock,
        fss_mock: MagicMock,
        fsi_mock: MagicMock,
        efo_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        df: DataFrame,
        harmonised: DataFrame,
    ) -> None:
        """Test step execution using mocks, with the outputs written to disk."""
        # Any dataset mock
        dataset = MagicMock()
        dataset.df = df
        paths = ["path1", "path2", "path3"]
        dataset.get_summary_statistics_paths = MagicMock(return_value=paths)
        dataset.annotate_sumstats_qc = MagicMock(return_value=dataset)
        harmonised_dataset = MagicMock()
        harmonised_dataset.df = harmonised

        fsi_mock.from_finngen_manifest = MagicMock(return_value=dataset)
        fss_mock.from_source_files = MagicMock(return_value=df)
        fss_mock.from_source = MagicMock(return_value=harmonised_dataset)
        hss_mock.from_parquet = MagicMock(return_value=dataset)
        qc_mock.from_summary_statistics = MagicMock(return_value=dataset)
        qc_mock.from_parquet = MagicMock(return_value=dataset)

        source_manifest_path = (tmp_path / "source_manifest").as_posix()
        efo_curation_path = (tmp_path / "efo_curation").as_posix()
        gnomad_variant_index_path = (tmp_path / "gnomad_variant_index").as_posix()
        study_index_output_path = (tmp_path / "study_index_output").as_posix()
        harmonised_summary_statistics_output_path = (
            tmp_path / "harmonised_summary_stats_output"
        ).as_posix()
        harmonised_summary_statistics_qc_output_path = (
            tmp_path / "harmonised_summary_stats_qc_output"
        ).as_posix()

        FinngenUkbMvpMetaSummaryStatisticsIngestionStep(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            gnomad_variant_index_path=gnomad_variant_index_path,
            study_index_output_path=study_index_output_path,
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
        fss_mock.from_source_files.assert_called_once_with(
            session=session,
            summary_statistics_paths=paths,
            datasource=manifest_mock.from_path.return_value.meta,
        )
        vi_mock.from_parquet.assert_called_once_with(
            session=session, path=gnomad_variant_index_path
        )
        vd_mock.from_variant_index.assert_called_once_with(
            variant_index=vi_mock.from_parquet.return_value
        )
        fss_mock.from_source.assert_called_once_with(
            raw_summary_statistics=df,
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
            session=session, path=harmonised_summary_statistics_output_path
        )
        qc_mock.from_summary_statistics.assert_called_once_with(
            gwas=hss_mock.from_parquet.return_value, pval_threshold=1e-8
        )
        # The QC is computed once and read back for the study index.
        qc_mock.from_parquet.assert_called_once_with(
            session=session, path=harmonised_summary_statistics_qc_output_path
        )
        dataset.annotate_sumstats_qc.assert_called_once_with(
            qc_mock.from_parquet.return_value
        )
        assert Path(study_index_output_path).is_dir()
        assert Path(harmonised_summary_statistics_qc_output_path).is_dir()

        # One file per study and chromosome, sorted by position.
        output = Path(harmonised_summary_statistics_output_path)
        partitions = sorted(
            p.relative_to(output).as_posix() for p in output.glob("*/*") if p.is_dir()
        )
        assert partitions == [
            "studyId=S1/chromosome=1",
            "studyId=S1/chromosome=2",
            "studyId=S2/chromosome=1",
            "studyId=S2/chromosome=2",
        ]
        for partition in partitions:
            files = list((output / partition).glob("*.parquet"))
            assert len(files) == 1, f"{partition} has {len(files)} files"
            positions = [
                row.position
                for row in session.spark.read.parquet(files[0].as_posix()).collect()
            ]
            assert positions == [100, 200, 300], f"{partition} is not sorted"

        # The partition columns are read back from the directory names.
        written = SummaryStatistics.from_parquet(session, output.as_posix())
        assert written.df.count() == 12
        assert written.df.filter("studyId IS NULL OR chromosome IS NULL").count() == 0
