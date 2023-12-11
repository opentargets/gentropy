"""Test Thurman."""
from __future__ import annotations

import pytest
from otg.common.Liftover import LiftOverSpark
from otg.dataset.gene_index import GeneIndex
from otg.datasource.intervals.thurman import IntervalsThurman
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def sample_intervals_thurman(spark: SparkSession) -> DataFrame:
    """Sample Thurman intervals."""
    return IntervalsThurman.read(spark, "tests/data_samples/thurman_sample.bed8")


def test_read_thurman(sample_intervals_thurman: DataFrame) -> None:
    """Test read Thurman data."""
    assert isinstance(sample_intervals_thurman, DataFrame)


def test_thurman_intervals_from_source(
    sample_intervals_thurman: DataFrame,
    mock_gene_index: GeneIndex,
    liftover_chain_37_to_38: LiftOverSpark,
) -> None:
    """Test IntervalsThurman creation with mock data."""
    assert isinstance(
        IntervalsThurman.parse(
            sample_intervals_thurman, mock_gene_index, liftover_chain_37_to_38
        ),
        IntervalsThurman,
    )
