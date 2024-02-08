"""Test LD annotation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.ld import LDAnnotator
from pyspark.sql import Row

if TYPE_CHECKING:
    from gentropy.dataset.ld_index import LDIndex
    from gentropy.dataset.study_index import StudyIndex
    from pyspark.sql import SparkSession


class TestLDAnnotator:
    """Test LDAnnotatorGnomad."""

    def test__add_population_size(
        self: TestLDAnnotator,
    ) -> None:
        """Test _add_population_size."""
        result_df = self.observed_df.select(
            LDAnnotator._add_population_size(
                f.col("ldSet"), f.col("ldPopulationStructure")
            ).alias("ldSet")
        )
        expected = [0.8, None]
        for i, row in enumerate(result_df.collect()):
            assert row["ldSet"][0]["rValues"][i]["relativeSampleSize"] == pytest.approx(
                expected[i]
            )

    def test__calculate_weighted_r_overall(
        self: TestLDAnnotator,
    ) -> None:
        """Test _calculate_weighted_r_overall."""
        result_df = self.observed_df.withColumn(
            "ldSet",
            LDAnnotator._add_population_size(
                f.col("ldSet"), f.col("ldPopulationStructure")
            ),
        ).withColumn("ldSet", LDAnnotator._calculate_weighted_r_overall(f.col("ldSet")))
        expected = 0.2
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
