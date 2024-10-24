"""Test JavierreIntervals."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from gentropy.common.Liftover import LiftOverSpark
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.intervals import Intervals
from gentropy.datasource.intervals.javierre import IntervalsJavierre


@pytest.fixture(scope="module")
def sample_intervals_javierre(spark: SparkSession) -> DataFrame:
    """Sample Javierre intervals."""
    return IntervalsJavierre.read(
        spark, "tests/gentropy/data_samples/javierre_sample.parquet"
    )


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
