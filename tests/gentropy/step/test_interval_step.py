"""Integration test for interval step."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.dataset.dataset import DatasetValidationResult
from gentropy.intervals import IntervalE2GStep, IntervalEpiractionStep


class TestIntervalE2GStep:
    """Test Interval E2G Step."""

    @pytest.mark.step_test
    @patch("gentropy.intervals.BiosampleIndex")
    @patch("gentropy.intervals.ContigIndex")
    @patch("gentropy.intervals.TargetIndex")
    @patch("gentropy.intervals.IntervalsE2G")
    @patch("pyspark.sql.readwriter.DataFrameReader.csv")
    def test_interval_e2g_step(
        self,
        spark_read: MagicMock,
        intervals_e2g: MagicMock,
        target: MagicMock,
        contig: MagicMock,
        biosample: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Test Interval E2G Step callstack."""
        target_index_path = (tmp_path / "target").as_posix()
        biosample_mapping_path = (tmp_path / "biosample_mapping.tsv").as_posix()
        biosample_index_path = (tmp_path / "biosample").as_posix()
        contig_index_path = (tmp_path / "contig").as_posix()
        interval_source = (tmp_path / "e2g_source").as_posix()
        valid_output_path = (tmp_path / "valid_intervals").as_posix()
        invalid_output_path = (tmp_path / "invalid_intervals").as_posix()
        min_valid_score = 0.5
        max_valid_score = 1.0
        invalid_qc_reasons = ["INVALID_CHROMOSOME", "INVALID_SCORE"]

        dummy_df = session.spark.createDataFrame(
            [("A", 1), ("C", 2)], schema="chromosome STRING, start LONG"
        )

        # Mock the `Dataset.from_parquet` methods to return MagicMock instances
        target.from_parquet = MagicMock()
        target.from_parquet.persist = MagicMock()
        biosample.from_parquet = MagicMock()
        contig.from_parquet = MagicMock()

        # Mock spark_read return value to return dummy_df
        spark_read.return_value = dummy_df

        # Mock E2G.read to return dummy_df
        intervals_e2g.read = MagicMock(return_value=dummy_df)
        intervals_instance = MagicMock()
        intervals_instance.df = dummy_df
        intervals_instance.qc = MagicMock(
            return_value=DatasetValidationResult(
                valid=intervals_instance, invalid=intervals_instance
            )
        )
        intervals_e2g.parse = MagicMock(return_value=intervals_instance)

        IntervalE2GStep(
            session=session,
            target_index_path=target_index_path,
            biosample_mapping_path=biosample_mapping_path,
            biosample_index_path=biosample_index_path,
            chromosome_contig_index_path=contig_index_path,
            interval_source=interval_source,
            valid_output_path=valid_output_path,
            invalid_output_path=invalid_output_path,
            min_valid_score=min_valid_score,
            max_valid_score=max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )

        # Assert the biosample mapping was read via csv file
        spark_read.assert_called_once_with(biosample_mapping_path, header=True)
        # Assert the datasets were read
        target.from_parquet.assert_called_once_with(session, target_index_path)
        target.from_parquet.return_value.persist.assert_called_once()

        biosample.from_parquet.assert_called_once_with(session, biosample_index_path)
        contig.from_parquet.assert_called_once_with(session, contig_index_path)
        # Assert the read method was called for E2G data
        intervals_e2g.read.assert_called_once_with(session.spark, interval_source)
        # Assert the parse method was called for E2G data
        intervals_e2g.parse.assert_called_once_with(
            intervals_e2g.read.return_value,
            spark_read.return_value,
            target.from_parquet.return_value.persist.return_value,
        )
        # Assert the qc method was called
        intervals_instance.qc.assert_called_once_with(
            contig_index=contig.from_parquet.return_value,
            target_index=target.from_parquet.return_value.persist.return_value,
            biosample_index=biosample.from_parquet.return_value,
            min_valid_score=min_valid_score,
            max_valid_score=max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )
        # Assert that valid and invalid datasets were written
        assert Path(valid_output_path).exists()
        assert Path(invalid_output_path).exists()


class TestIntervalEpiractionStep:
    """Test Interval Epiraction Step."""

    @pytest.mark.step_test
    @patch("gentropy.intervals.BiosampleIndex")
    @patch("gentropy.intervals.ContigIndex")
    @patch("gentropy.intervals.TargetIndex")
    @patch("gentropy.intervals.IntervalsEpiraction")
    def test_interval_epiraction_step(
        self,
        intervals_epiraction: MagicMock,
        target: MagicMock,
        contig: MagicMock,
        biosample: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Test Interval Epiraction Step callstack."""
        target_index_path = (tmp_path / "target").as_posix()
        biosample_index_path = (tmp_path / "biosample").as_posix()
        contig_index_path = (tmp_path / "contig").as_posix()
        interval_source = (tmp_path / "epiraction_source").as_posix()
        valid_output_path = (tmp_path / "valid_intervals").as_posix()
        invalid_output_path = (tmp_path / "invalid_intervals").as_posix()
        min_valid_score = 0.5
        max_valid_score = 1.0
        invalid_qc_reasons = ["INVALID_CHROMOSOME", "INVALID_SCORE"]

        dummy_df = session.spark.createDataFrame(
            [("A", 0), ("C", 1)], schema="chromosome STRING, start LONG"
        )

        # Mock the `Dataset.from_parquet` methods to return MagicMock instances
        target.from_parquet = MagicMock()
        target.from_parquet.persist = MagicMock()
        biosample.from_parquet = MagicMock()
        contig.from_parquet = MagicMock()

        # Mock Epiraction.read to return dummy_df
        intervals_epiraction.read = MagicMock(return_value=dummy_df)
        intervals_instance = MagicMock()
        intervals_instance.df = dummy_df
        intervals_instance.qc = MagicMock(
            return_value=DatasetValidationResult(
                valid=intervals_instance, invalid=intervals_instance
            )
        )
        intervals_epiraction.parse = MagicMock(return_value=intervals_instance)

        IntervalEpiractionStep(
            session=session,
            target_index_path=target_index_path,
            biosample_index_path=biosample_index_path,
            chromosome_contig_index_path=contig_index_path,
            interval_source=interval_source,
            valid_output_path=valid_output_path,
            invalid_output_path=invalid_output_path,
            min_valid_score=min_valid_score,
            max_valid_score=max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )

        # Assert the datasets were read
        target.from_parquet.assert_called_once_with(session, target_index_path)
        target.from_parquet.return_value.persist.assert_called_once()
        biosample.from_parquet.assert_called_once_with(session, biosample_index_path)
        contig.from_parquet.assert_called_once_with(session, contig_index_path)
        # Assert the read method was called for Epiraction data
        intervals_epiraction.read.assert_called_once_with(
            session.spark, interval_source
        )
        # Assert the parse method was called for Epiraction data
        intervals_epiraction.parse.assert_called_once_with(
            intervals_epiraction.read.return_value,
            target.from_parquet.return_value.persist.return_value,
        )
        # Assert the qc method was called
        intervals_instance.qc.assert_called_once_with(
            contig_index=contig.from_parquet.return_value,
            target_index=target.from_parquet.return_value.persist.return_value,
            biosample_index=biosample.from_parquet.return_value,
            min_valid_score=min_valid_score,
            max_valid_score=max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )
        # Assert that valid and invalid datasets were written
        assert Path(valid_output_path).exists()
        assert Path(invalid_output_path).exists()
