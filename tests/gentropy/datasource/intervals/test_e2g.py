"""Test E2G Intervals."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.intervals.e2g import IntervalsE2G


@pytest.fixture(scope="module")
def sample_intervals_e2g(spark: SparkSession) -> DataFrame:
    """Sample E2G intervals."""
    return IntervalsE2G.read(spark, "tests/gentropy/data_samples/e2g_sample.bed")


@pytest.fixture(scope="module")
def sample_biosample_mapping(spark: SparkSession) -> DataFrame:
    """Sample E2G biosample mapping."""
    return spark.read.csv(
        "tests/gentropy/data_samples/biosample_mapping_sample.tsv",
        sep="\t",
        header=True,
    )


def test_read_e2g(sample_intervals_e2g: DataFrame) -> None:
    """Test read E2G data."""
    assert isinstance(sample_intervals_e2g, DataFrame)


def test_e2g_intervals_from_source(
    sample_intervals_e2g: DataFrame,
    sample_biosample_mapping: DataFrame,
    mock_target_index: TargetIndex,
    mock_biosample_index: BiosampleIndex,
) -> None:
    """Test E2GIntervals creation with mock data."""
    assert isinstance(
        IntervalsE2G.parse(
            sample_intervals_e2g,
            sample_biosample_mapping,
            mock_target_index,
            mock_biosample_index,
        ),
        Intervals,
    )
