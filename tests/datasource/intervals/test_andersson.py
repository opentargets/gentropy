"""Test Andersson Intervals."""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from otg.common.Liftover import LiftOverSpark
from otg.dataset.gene_index import GeneIndex
from otg.datasource.intervals.andersson import IntervalsAndersson


@pytest.fixture(scope="module")
def sample_intervals_andersson(spark: SparkSession) -> DataFrame:
    """Sample Andersson intervals."""
    return IntervalsAndersson.read_andersson(
        spark, "tests/data_samples/andersson_sample.bed"
    )


def test_read_andersson(sample_intervals_andersson: DataFrame) -> None:
    """Test read_andersson."""
    assert isinstance(sample_intervals_andersson, DataFrame)


def test_andersson_intervals_from_source(
    sample_intervals_andersson: DataFrame,
    mock_gene_index: GeneIndex,
    liftover_chain_37_to_38: LiftOverSpark,
) -> None:
    """Test JavierreIntervals creation with mock data."""
    assert isinstance(
        IntervalsAndersson.parse(
            sample_intervals_andersson, mock_gene_index, liftover_chain_37_to_38
        ),
        IntervalsAndersson,
    )
