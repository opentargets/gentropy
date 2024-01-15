"""Test JavierreIntervals."""
from __future__ import annotations

import pytest
from oxygen.common.Liftover import LiftOverSpark
from oxygen.dataset.gene_index import GeneIndex
from oxygen.dataset.intervals import Intervals
from oxygen.datasource.intervals.javierre import IntervalsJavierre
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def sample_intervals_javierre(spark: SparkSession) -> DataFrame:
    """Sample Javierre intervals."""
    return IntervalsJavierre.read(spark, "tests/data_samples/javierre_sample.parquet")


def test_read_javierre(sample_intervals_javierre: DataFrame) -> None:
    """Test read javierre data."""
    assert isinstance(sample_intervals_javierre, DataFrame)


def test_javierre_intervals_from_source(
    sample_intervals_javierre: DataFrame,
    mock_gene_index: GeneIndex,
    liftover_chain_37_to_38: LiftOverSpark,
) -> None:
    """Test JavierreIntervals creation with mock data."""
    assert isinstance(
        IntervalsJavierre.parse(
            sample_intervals_javierre, mock_gene_index, liftover_chain_37_to_38
        ),
        Intervals,
    )
