"""Test BigBrain ingestion steps."""

from __future__ import annotations

import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.bigbrain_ingestion import (
    BigBrainSummaryStatisticsHarmonisationStep,
    BigBrainSummaryStatisticsIngestionStep,
)
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.bigbrain.summary_statistics import FULL_ASSOC_SCHEMA


@pytest.fixture
def bigbrain_raw_full_assoc_df(session: Session) -> DataFrame:
    """Raw full_assoc example DataFrame fixture (already schema-aligned)."""
    data = [
        Row(feature="ENSG00000177757.2"),
        Row(feature="ENSG00000187608.9"),
    ]
    return session.spark.createDataFrame(data)


@pytest.fixture
def bigbrain_summary_statistics_df(session: Session) -> DataFrame:
    """Harmonised SummaryStatistics example DataFrame fixture.

    Values are given as a positional tuple (rather than keyword `Row(...)`, whose
    kwargs get alphabetically reordered by PySpark) so they line up with the field
    order declared by `SummaryStatistics.get_schema()`.
    """
    schema = SummaryStatistics.get_schema()
    data = [
        (
            "BigBrain_eqtl_EUR_ENSG00000177757.2",  # studyId
            "1_100_A_G",  # variantId
            "1",  # chromosome
            100,  # position
            0.5,  # beta
            10_725,  # sampleSize
            1.5,  # pValueMantissa
            -8,  # pValueExponent
            None,  # effectAlleleFrequencyFromSource
            0.1,  # standardError
        ),
    ]
    return session.spark.createDataFrame(data, schema=schema)


class TestBigBrainSummaryStatisticsIngestionStep:
    """Test BigBrainSummaryStatisticsIngestionStep."""

    @patch("gentropy.bigbrain_ingestion.BigBrainSummaryStatistics")
    def test_bigbrain_summary_statistics_ingestion_step(
        self,
        summary_statistics_mock: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Test that the ingestion step downloads both files and writes raw parquet."""
        raw_full_assoc_path = (tmp_path / "raw_full_assoc").as_posix()
        raw_top_assoc_path = (tmp_path / "raw_top_assoc").as_posix()

        # `bigbrain_ingestion` imports FULL_ASSOC_SCHEMA directly (not via the
        # mocked BigBrainSummaryStatistics class), so the real schema is used
        # for the Spark read regardless of this patch.
        header = "\t".join(field.name for field in FULL_ASSOC_SCHEMA)

        def fake_download(url: str, output_path: str, session: Session) -> None:
            """Write a minimal real gzipped TSV so the subsequent Spark read succeeds."""
            content = gzip.compress(f"{header}\n".encode())
            with open(output_path, "wb") as fh:
                fh.write(content)

        summary_statistics_mock.download_tsv_gz.side_effect = fake_download

        BigBrainSummaryStatisticsIngestionStep(
            session=session,
            full_assoc_url="https://zenodo.org/records/17226890/files/full_assoc.tsv.gz/content",
            top_assoc_url="https://zenodo.org/records/17226890/files/top_assoc.tsv.gz/content",
            raw_full_assoc_path=raw_full_assoc_path,
            raw_top_assoc_path=raw_top_assoc_path,
        )

        assert summary_statistics_mock.download_tsv_gz.call_count == 2
        assert Path(raw_full_assoc_path).exists()
        assert Path(raw_top_assoc_path).exists()


class TestBigBrainSummaryStatisticsHarmonisationStep:
    """Test BigBrainSummaryStatisticsHarmonisationStep."""

    @patch("gentropy.bigbrain_ingestion.BigBrainStudyIndex")
    @patch("gentropy.bigbrain_ingestion.BigBrainSummaryStatistics")
    def test_bigbrain_harmonisation_step_eqtl(
        self,
        summary_statistics_mock: MagicMock,
        study_index_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        bigbrain_raw_full_assoc_df: DataFrame,
        bigbrain_summary_statistics_df: DataFrame,
    ) -> None:
        """For eqtl, the harmonisation step should use gene_map_from_feature (not top_assoc)."""
        raw_full_assoc_path = (tmp_path / "raw_full_assoc").as_posix()
        raw_top_assoc_path = (tmp_path / "raw_top_assoc").as_posix()
        harmonised_summary_statistics_path = (
            tmp_path / "harmonised_summary_statistics"
        ).as_posix()
        study_index_path = (tmp_path / "study_index").as_posix()

        bigbrain_raw_full_assoc_df.write.mode("overwrite").parquet(raw_full_assoc_path)

        harmonised_instance = MagicMock()
        harmonised_instance.df = bigbrain_summary_statistics_df
        summary_statistics_mock.from_source.return_value = harmonised_instance

        study_index_instance = MagicMock()
        study_index_instance.df = bigbrain_summary_statistics_df
        study_index_mock.from_source.return_value = study_index_instance

        BigBrainSummaryStatisticsHarmonisationStep(
            session=session,
            qtl_type="eqtl",
            raw_full_assoc_path=raw_full_assoc_path,
            raw_top_assoc_path=raw_top_assoc_path,
            harmonised_summary_statistics_path=harmonised_summary_statistics_path,
            study_index_path=study_index_path,
        )

        summary_statistics_mock.from_source.assert_called_once()
        study_index_mock.gene_map_from_feature.assert_called_once()
        study_index_mock.gene_map_from_top_assoc.assert_not_called()
        assert Path(harmonised_summary_statistics_path).exists()
        assert Path(study_index_path).exists()

    @patch("gentropy.bigbrain_ingestion.BigBrainStudyIndex")
    @patch("gentropy.bigbrain_ingestion.BigBrainSummaryStatistics")
    def test_bigbrain_harmonisation_step_sqtl_uses_top_assoc(
        self,
        summary_statistics_mock: MagicMock,
        study_index_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        bigbrain_raw_full_assoc_df: DataFrame,
        bigbrain_summary_statistics_df: DataFrame,
    ) -> None:
        """For sqtl, the harmonisation step should read raw_top_assoc_path and use gene_map_from_top_assoc."""
        raw_full_assoc_path = (tmp_path / "raw_full_assoc").as_posix()
        raw_top_assoc_path = (tmp_path / "raw_top_assoc").as_posix()
        harmonised_summary_statistics_path = (
            tmp_path / "harmonised_summary_statistics"
        ).as_posix()
        study_index_path = (tmp_path / "study_index").as_posix()

        bigbrain_raw_full_assoc_df.write.mode("overwrite").parquet(raw_full_assoc_path)
        bigbrain_raw_full_assoc_df.write.mode("overwrite").parquet(raw_top_assoc_path)

        harmonised_instance = MagicMock()
        harmonised_instance.df = bigbrain_summary_statistics_df
        summary_statistics_mock.from_source.return_value = harmonised_instance

        study_index_instance = MagicMock()
        study_index_instance.df = bigbrain_summary_statistics_df
        study_index_mock.from_source.return_value = study_index_instance

        BigBrainSummaryStatisticsHarmonisationStep(
            session=session,
            qtl_type="sqtl",
            raw_full_assoc_path=raw_full_assoc_path,
            raw_top_assoc_path=raw_top_assoc_path,
            harmonised_summary_statistics_path=harmonised_summary_statistics_path,
            study_index_path=study_index_path,
        )

        study_index_mock.gene_map_from_top_assoc.assert_called_once()
        study_index_mock.gene_map_from_feature.assert_not_called()
        assert Path(harmonised_summary_statistics_path).exists()
        assert Path(study_index_path).exists()
