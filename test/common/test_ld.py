"""Tests for the linkage disequilibrium helpers."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.common.ld import major_population


@pytest.mark.legacy
@pytest.mark.parametrize(
    ("structure", "expected"),
    [
        ([("pop1", 0.5), ("pop2", 0.3), ("pop3", 0.2)], "pop1"),
        ([("pop1", 0.4), ("pop2", 0.4), ("pop3", 0.2)], "pop1"),
        ([("pop1", 0.4), ("nfe", 0.4), ("pop3", 0.2)], "nfe"),
    ],
    ids=["no-tie", "tie-without-nfe", "tie-with-nfe"],
)
def test_major_population(
    spark: SparkSession, structure: list[tuple[str, float]], expected: str
) -> None:
    """The largest population wins; a tie goes to nfe, else the first by name."""
    df = spark.createDataFrame(
        [(structure,)],
        "ldPopulationStructure array<struct<ldPopulation: string, "
        "relativeSampleSize: double>>",
    )

    result = df.select(major_population(f.col("ldPopulationStructure"))).first()

    assert result == (expected,)
