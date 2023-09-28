"""Test Thurman."""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from otg.common.Liftover import LiftOverSpark
from otg.dataset.gene_index import GeneIndex
from otg.datasource.intervals.thurnman import IntervalsThurnman


@pytest.fixture(scope="module")
def sample_intervals_thurnman(spark: SparkSession) -> DataFrame:
    """Sample Andersson intervals."""
    return IntervalsThurnman.read_thurnman(
        spark, "tests/data_samples/thurnman_sample.bed8"
    )


def test_read_thurnman(sample_intervals_thurnman: DataFrame) -> None:
    """Test read_jung."""
    assert isinstance(sample_intervals_thurnman, DataFrame)


def test_thurnman_intervals_from_source(
    sample_intervals_thurnman: DataFrame,
    mock_gene_index: GeneIndex,
    liftover_chain_37_to_38: LiftOverSpark,
) -> None:
    """Test IntervalsThurnman creation with mock data."""
    assert isinstance(
        IntervalsThurnman.parse(
            sample_intervals_thurnman, mock_gene_index, liftover_chain_37_to_38
        ),
        IntervalsThurnman,
    )
