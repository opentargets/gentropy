"""Test clumping methods."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest

from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.clump import LDclumping

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(LDclumping.clump(mock_study_locus), StudyLocus)


@pytest.mark.parametrize(
    ("observed_data", "expected_data"),
    [
        (
            [
                (
                    # Dependent locus - lead is correlated with a more significant variant
                    1,
                    "L1",
                    "GCST005650_1",
                    1.0,
                    -17,
                    [{"tagVariantId": "T1"}, {"tagVariantId": "L2"}],
                    None,
                ),
                (
                    # Dependent locus - lead shows a stronger association than the row above
                    2,
                    "L2",
                    "GCST005650_1",
                    4.0,
                    -18,
                    [
                        {"tagVariantId": "T2"},
                        {"tagVariantId": "T3"},
                        {"tagVariantId": "L1"},
                    ],
                    None,
                ),
                (
                    # Independent locus
                    3,
                    "L2",
                    "GCST005650_1",
                    4.0,
                    -18,
                    [
                        {"tagVariantId": "L3"},
                        {"tagVariantId": "T4"},
                        {"tagVariantId": "L5"},
                    ],
                    None,
                ),
                (
                    # Empty credible set
                    4,
                    "L3",
                    "GCST005650_1",
                    4.0,
                    -18,
                    [],
                    None,
                ),
                (
                    # Null credible set
                    5,
                    "L4",
                    "GCST005650_1",
                    4.0,
                    -18,
                    None,
                    None,
                ),
            ],
            [
                (
                    # Signal is linked to the next row
                    1,
                    "L1",
                    "GCST005650_1",
                    1.0,
                    -17,
                    [{"tagVariantId": "T1"}, {"tagVariantId": "L2"}],
                    True,
                ),
                (
                    # Signal is the most significant
                    2,
                    "L2",
                    "GCST005650_1",
                    4.0,
                    -18,
                    [
                        {"tagVariantId": "T2"},
                        {"tagVariantId": "T3"},
                        {"tagVariantId": "L1"},
                    ],
                    False,
                ),
                (
                    # Signal is not linked
                    3,
                    "L2",
                    "GCST005650_1",
                    4.0,
                    -18,
                    [
                        {"tagVariantId": "L3"},
                        {"tagVariantId": "T4"},
                        {"tagVariantId": "L5"},
                    ],
                    False,
                ),
                (
                    # Empty credible set - signal is not linked
                    4,
                    "L3",
                    "GCST005650_1",
                    4.0,
                    -18,
                    [],
                    False,
                ),
                (
                    # Null credible set - signal is not linked
                    5,
                    "L4",
                    "GCST005650_1",
                    4.0,
                    -18,
                    None,
                    False,
                ),
            ],
        )
    ],
)
def test_is_lead_linked(
    spark: SparkSession, observed_data: list[Any], expected_data: list[Any]
) -> None:
    """Test function that annotates whether a studyLocusId is linked to a more statistically significant studyLocusId."""
    schema = t.StructType(
        [
            t.StructField("studyLocusId", t.LongType(), True),
            t.StructField("variantId", t.StringType(), True),
            t.StructField("studyId", t.StringType(), True),
            t.StructField("pValueMantissa", t.FloatType(), True),
            t.StructField("pValueExponent", t.IntegerType(), True),
            t.StructField(
                "ldSet",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("tagVariantId", t.StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            t.StructField("is_lead_linked", t.BooleanType(), True),
        ]
    )
    study_locus_df = spark.createDataFrame(
        observed_data,
        schema,
    )
    observed_df = (
        study_locus_df.withColumn(
            "is_lead_linked",
            LDclumping._is_lead_linked(
                f.col("studyId"),
                f.col("variantId"),
                f.col("pValueExponent"),
                f.col("pValueMantissa"),
                f.col("ldSet"),
            ),
        )
        .orderBy("studyLocusId")
        .collect()
    )
    expected_df = (
        spark.createDataFrame(expected_data, schema).orderBy("studyLocusId").collect()
    )
    assert observed_df == expected_df
