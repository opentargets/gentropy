"""Test study index dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture()
def mock_summary_statistics(spark: SparkSession) -> SummaryStatistics:
    """Generating a mock summary statistics dataset."""
    schema = parse_spark_schema("summary_statistics.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="summaryStats",
        )
        .withSchema(schema)
        # Allowing missingness in effect allele frequency and enforce upper limit:
        .withColumnSpec(
            "effectAlleleFrequencyFromSource", percentNulls=0.1, maxValue=1.0
        )
        # Allowing missingness:
        .withColumnSpec("variantId", percentNulls=0.1)
        .withColumnSpec("chromosome", percentNulls=0.1)
        .withColumnSpec("position", percentNulls=0.1)
        .withColumnSpec("beta", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalUpper", percentNulls=0.1)
        # Making sure p-values are below 1:
        .withColumnSpec(
            "pValueMantissa", minValue=1, maxValue=10, random=True, percentNulls=0.1
        )
        .withColumnSpec(
            "pValueExponent", minValue=-400, maxValue=-1, random=True, percentNulls=0.1
        )
    )

    return SummaryStatistics(_df=data_spec.build(), _schema=schema)


def test_summary_statistics_creation(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_summary_statistics, SummaryStatistics)
