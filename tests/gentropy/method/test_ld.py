"""Test LD annotation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pyspark.sql import Row

from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.ld import LDAnnotator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gentropy.dataset.ld_index import LDIndex
    from gentropy.dataset.study_index import StudyIndex


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

    @pytest.mark.parametrize(
        ("observed", "expected"),
        [
            # r available for majorPopulation
            (
                # observed ldSet and majorPopulation
                [
                    Row(
                        majorPopulation="pop1",
                        ldSet=[
                            {
                                "tagVariantId": "tag1",
                                "rValues": [
                                    {"population": "pop1", "r": 0.5},
                                    {"population": "pop2", "r": 0.6},
                                ],
                            }
                        ],
                    )
                ],
                # expected r2Overall
                0.25,
            ),
            # r not available for majorPopulation
            (
                # observed ldSet and majorPopulation
                [
                    Row(
                        majorPopulation="pop3",
                        ldSet=[
                            {
                                "tagVariantId": "tag1",
                                "rValues": [
                                    {"population": "pop1", "r": 0.5},
                                    {"population": "pop2", "r": 0.6},
                                ],
                            }
                        ],
                    )
                ],
                # expected r2Overall
                0.0,
            ),
        ],
    )
    def test__calculate_r2_major(
        self: TestLDAnnotator,
        spark: SparkSession,
        observed: list[Any],
        expected: list[Any],
    ) -> None:
        """Test _calculate_r2_major."""
        schema = t.StructType(
            [
                t.StructField("majorPopulation", t.StringType(), True),
                t.StructField(
                    "ldSet",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("tagVariantId", t.StringType(), True),
                                t.StructField(
                                    "rValues",
                                    t.ArrayType(
                                        t.StructType(
                                            [
                                                t.StructField(
                                                    "population", t.StringType(), True
                                                ),
                                                t.StructField(
                                                    "r", t.DoubleType(), True
                                                ),
                                            ]
                                        )
                                    ),
                                    True,
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
            "ldSet",
            LDAnnotator._calculate_r2_major(f.col("ldSet"), f.col("majorPopulation")),
        )
        assert result_df.collect()[0]["ldSet"][0]["r2Overall"] == pytest.approx(
            expected
        )

    def test__rescue_lead_variant(self: TestLDAnnotator, spark: SparkSession) -> None:
        """Test _rescue_lead_variant."""
        observed_df = spark.createDataFrame(
            [("var1", None)],
            t.StructType(
                [
                    t.StructField("variantId", t.StringType(), True),
                    t.StructField(
                        "ldSet",
                        t.ArrayType(
                            t.StructType(
                                [
                                    t.StructField("tagVariantId", t.StringType(), True),
                                    t.StructField("r2Overall", t.DoubleType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
        )
        result_df = observed_df.withColumn(
            "ldSet",
            LDAnnotator._rescue_lead_variant(f.col("ldSet"), f.col("variantId")),
        )
        assert (
            result_df.select("ldSet.r2Overall").collect()[0]["r2Overall"][0] == 1.0
        ), "ldSet does not contain the rescued lead variant."

    @pytest.fixture(autouse=True)
    def _setup(self: TestLDAnnotator, spark: SparkSession) -> None:
        """Prepares fixtures for the test."""
        self.association_w_ld_set_schema = t.StructType(
            [
                t.StructField("variantId", t.StringType(), True),
                t.StructField(
                    "ldSet",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("tagVariantId", t.StringType(), True),
                                t.StructField(
                                    "rValues",
                                    t.ArrayType(
                                        t.StructType(
                                            [
                                                t.StructField(
                                                    "population", t.StringType(), True
                                                ),
                                                t.StructField(
                                                    "r", t.DoubleType(), True
                                                ),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
                t.StructField("studyId", t.StringType(), True),
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
                ),
            ]
        )
        observed_data = [
            Row(
                variantId="var1",
                ldSet=[
                    {
                        "tagVariantId": "tag1",
                        "rValues": [
                            {"population": "pop1", "r": 0.5},
                            {"population": "pop2", "r": 0.6},
                        ],
                    }
                ],
                studyId="study1",
                ldPopulationStructure=[
                    {
                        "ldPopulation": "pop1",
                        "relativeSampleSize": 0.8,
                    },
                    {
                        "ldPopulation": "pop3",
                        "relativeSampleSize": 0.2,
                    },
                ],
            ),
        ]
        self.observed_df = spark.createDataFrame(
            observed_data, self.association_w_ld_set_schema
        )

    def test_ldannotate(
        self: TestLDAnnotator,
        mock_study_locus: StudyLocus,
        mock_study_index: StudyIndex,
        mock_ld_index: LDIndex,
    ) -> None:
        """Test LD annotator."""
        assert isinstance(
            LDAnnotator.ld_annotate(mock_study_locus, mock_study_index, mock_ld_index),
            StudyLocus,
        )
