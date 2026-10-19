"""Test major population selection."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pyspark.sql import Row

from gentropy.method.ld import LDAnnotator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestLDAnnotator:
    """Test LDAnnotatorGnomad."""

    @pytest.mark.parametrize(
        ("observed", "expected"),
        [
            # no tie in relativeSampleSize
            (
                # observed ldPopulationStructure
                [
                    Row(
                        ldPopulationStructure=[
                            {"ldPopulation": "pop1", "relativeSampleSize": 0.5},
                            {"ldPopulation": "pop2", "relativeSampleSize": 0.3},
                            {"ldPopulation": "pop3", "relativeSampleSize": 0.2},
                        ],
                    )
                ],
                # expected majorPopulation
                "pop1",
            ),
            # tie in relativeSampleSize, "nfe" is not one of the tied populations
            (
                # observed ldPopulationStructure
                [
                    Row(
                        ldPopulationStructure=[
                            {"ldPopulation": "pop1", "relativeSampleSize": 0.4},
                            {"ldPopulation": "pop2", "relativeSampleSize": 0.4},
                            {"ldPopulation": "pop3", "relativeSampleSize": 0.2},
                        ],
                    )
                ],
                # expected majorPopulation
                "pop1",
            ),
            # tie in relativeSampleSize, "nfe" is one of the tied populations
            (
                # observed ldPopulationStructure
                [
                    Row(
                        ldPopulationStructure=[
                            {"ldPopulation": "pop1", "relativeSampleSize": 0.4},
                            {"ldPopulation": "nfe", "relativeSampleSize": 0.4},
                            {"ldPopulation": "pop3", "relativeSampleSize": 0.2},
                        ],
                    )
                ],
                # expected majorPopulation
                "nfe",
            ),
        ],
    )
    def test__get_major_population(
        self: TestLDAnnotator,
        spark: SparkSession,
        observed: list[Any],
        expected: list[Any],
    ) -> None:
        """Test _get_major_population."""
        schema = t.StructType(
            [
                t.StructField(
                    "ldPopulationStructure",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("ldPopulation", t.StringType(), True),
                                t.StructField(
                                    "relativeSampleSize", t.DoubleType(), True
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        observed_df = spark.createDataFrame(observed, schema)
        result_df = observed_df.withColumn(
            "majorPopulation",
            LDAnnotator._get_major_population(f.col("ldPopulationStructure")),
        )
        assert result_df.collect()[0]["majorPopulation"] == pytest.approx(expected)
