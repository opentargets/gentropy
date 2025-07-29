"""Test Andersson Intervals."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.intervals.e2g import IntervalsE2G


@pytest.fixture(scope="module")
def sample_intervals_e2g(spark: SparkSession) -> DataFrame:
    """Sample E2G intervals."""
    return IntervalsE2G.read(spark, "tests/gentropy/data_samples/e2g_sample.bed")


def test_read_e2g(sample_intervals_e2g: DataFrame) -> None:
    """Test read E2G data."""
    assert isinstance(sample_intervals_e2g, DataFrame)


def test_e2g_intervals_from_source(
    sample_intervals_e2g: DataFrame,
    mock_biosample_mapping: DataFrame,
    mock_target_index: TargetIndex,
) -> None:
    """Test E2GIntervals creation with mock data."""
    assert isinstance(
        IntervalsE2G.parse(
            sample_intervals_e2g, mock_biosample_mapping, mock_target_index
        ),
        Intervals,
    )
