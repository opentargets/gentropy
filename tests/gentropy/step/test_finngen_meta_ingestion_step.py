"""Test FinnGen UKBB MVP meta ingestion steps."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.finngen_ukb_mvp_meta import (
    FinngenUkbMvpMetaStudyIndexStep,
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
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenUkbMvpMetaSummaryStatistics")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.VariantDirection")
    def test_step(
        self,
        vd_mock: MagicMock,
        vi_mock: MagicMock,
        fss_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        df: DataFrame,
        harmonised: DataFrame,
    ) -> None:
        """Test step execution using mocks, with the harmonised summary statistics written to disk."""
        paths = ["path1", "path2", "path3"]
        manifest = manifest_mock.from_path.return_value
        manifest.get_summary_statistics_paths = MagicMock(return_value=paths)
        harmonised_dataset = MagicMock()
        harmonised_dataset.df = harmonised
        fss_mock.from_source_files = MagicMock(return_value=df)
        fss_mock.from_source = MagicMock(return_value=harmonised_dataset)

        source_manifest_path = (tmp_path / "source_manifest").as_posix()
        gnomad_variant_index_path = (tmp_path / "gnomad_variant_index").as_posix()
        output = tmp_path / "harmonised_summary_stats_output"

        FinngenUkbMvpMetaSummaryStatisticsIngestionStep(
            session=session,
            source_manifest_path=source_manifest_path,
            gnomad_variant_index_path=gnomad_variant_index_path,
            harmonised_summary_statistics_output_path=output.as_posix(),
            perform_meta_analysis_filter=True,
            imputation_score_threshold=0.8,
            perform_imputation_score_filter=True,
            perform_min_allele_count_filter=True,
            min_allele_count_threshold=20,
            perform_min_allele_frequency_filter=True,
            min_allele_frequency_threshold=0.01,
            filter_out_ambiguous_variants=True,
        )

        manifest_mock.from_path.assert_called_once_with(
            session=session, manifest_path=source_manifest_path
        )
        fss_mock.from_source_files.assert_called_once_with(
            session=session,
            summary_statistics_paths=paths,
            datasource=manifest.meta,
        )
        vi_mock.from_parquet.assert_called_once_with(
            session=session, path=gnomad_variant_index_path
        )
        vd_mock.from_variant_index.assert_called_once_with(
            variant_index=vi_mock.from_parquet.return_value
        )
        fss_mock.from_source.assert_called_once_with(
            raw_summary_statistics=df,
            finngen_manifest=manifest,
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

        # One file per study and chromosome, sorted by position.
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


class TestFinnGenMetaStudyIndexStep:
    """Test suite for FinnGen UKBB MVP meta study index step."""

    @pytest.mark.step_test
    @pytest.mark.parametrize("with_qc", [True, False], ids=["with qc", "without qc"])
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaManifest")
    @patch("gentropy.finngen_ukb_mvp_meta.EFOMapping")
    @patch("gentropy.finngen_ukb_mvp_meta.FinnGenMetaStudyIndex")
    @patch("gentropy.finngen_ukb_mvp_meta.SummaryStatisticsQC")
    def test_step(
        self,
        qc_mock: MagicMock,
        fsi_mock: MagicMock,
        efo_mock: MagicMock,
        manifest_mock: MagicMock,
        with_qc: bool,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Test that the study index is annotated with the QC only when a QC path is given."""
        study_index = MagicMock()
        study_index.df = session.spark.createDataFrame([("S1",)], "studyId STRING")
        study_index.annotate_sumstats_qc = MagicMock(return_value=study_index)
        fsi_mock.from_finngen_manifest = MagicMock(return_value=study_index)
        source_manifest_path = (tmp_path / "source_manifest").as_posix()
        efo_curation_path = (tmp_path / "efo_curation").as_posix()
        sumstats_qc_path = (tmp_path / "qc").as_posix() if with_qc else None
        output = tmp_path / "study_index"

        FinngenUkbMvpMetaStudyIndexStep(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            study_index_output_path=output.as_posix(),
            sumstats_qc_path=sumstats_qc_path,
        )

        fsi_mock.from_finngen_manifest.assert_called_once_with(
            manifest=manifest_mock.from_path.return_value,
            efo_mapping=efo_mock.from_path.return_value,
        )
        if with_qc:
            qc_mock.from_parquet.assert_called_once_with(
                session=session, path=sumstats_qc_path
            )
            study_index.annotate_sumstats_qc.assert_called_once_with(
                qc_mock.from_parquet.return_value
            )
        else:
            qc_mock.from_parquet.assert_not_called()
            study_index.annotate_sumstats_qc.assert_not_called()
        assert session.spark.read.parquet(output.as_posix()).count() == 1
