"""Test study locus overlap dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from otg.dataset.study_index import StudyIndex
    from pyspark.sql import SparkSession


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test study locus overlap creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_study_locus_overlap_from_associations(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(mock_study_locus, mock_study_index)
    assert isinstance(overlaps, StudyLocusOverlap)


@pytest.mark.parametrize(
    "observed, expected",
    [
        (
            [  # observed - input DataFrame representing gwas and nongwas data to find overlapping signals
                (
                    1,
                    "var1",
                    "study1",
                    "gwas",
                    "1",
                    [
                        {"posteriorProbability": 0.8, "variantId": "var3"},
                        {"posteriorProbability": 0.8, "variantId": "var4"},
                    ],
                    ["var3", "var4"],
                ),
                (
                    2,
                    "var2",
                    "study2",
                    "eqtl",
                    "1",
                    [{"posteriorProbability": 0.7, "variantId": "var3"}],
                    ["var3"],
                ),
            ],
            ["var3"],  # expected - overlapping variants
        ),
    ],
)
def test__find_overlaps(
    spark: SparkSession, observed: list[Any], expected: list[str]
) -> None:
    """Test _find_overlaps logi."""
    mock_schema = t.StructType(
        [
            t.StructField("studyLocusId", t.LongType(), nullable=False),
            t.StructField("variantId", t.StringType(), nullable=False),
            t.StructField("studyId", t.StringType(), nullable=False),
            t.StructField("studyType", t.StringType(), nullable=False),
            t.StructField("chromosome", t.StringType(), nullable=False),
            t.StructField(
                "locus",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField(
                                "is95CredibleSet", t.BooleanType(), nullable=True
                            ),
                            t.StructField(
                                "is99CredibleSet", t.BooleanType(), nullable=True
                            ),
                            t.StructField("logABF", t.DoubleType(), nullable=True),
                            t.StructField(
                                "posteriorProbability", t.DoubleType(), nullable=True
                            ),
                            t.StructField("variantId", t.StringType(), nullable=True),
                            t.StructField(
                                "pValueMantissa", t.FloatType(), nullable=True
                            ),
                            t.StructField(
                                "pValueExponent", t.IntegerType(), nullable=True
                            ),
                            t.StructField("beta", t.DoubleType(), nullable=True),
                            t.StructField(
                                "standardError", t.DoubleType(), nullable=True
                            ),
                            t.StructField("r2Overall", t.DoubleType(), nullable=True),
                        ]
                    )
                ),
                nullable=True,
            ),
            t.StructField(
                "variants_in_locus", t.ArrayType(t.StringType()), nullable=True
            ),
        ]
    )
    observed_df = spark.createDataFrame(observed, mock_schema)
    result_df = StudyLocus._find_overlaps(observed_df)
    assert (
        result_df.select(
            (f.size("leftLocus") == f.size("rightLocus")).alias("sameSize")
        )
        .distinct()
        .collect()[0]["sameSize"]
        is True
    ), "Both locus should contain the same number of variants."
    assert (
        result_df.select(f.col("leftLocus.variantId")).collect()[0]["variantId"]
        == expected
    ), "Overlapping variant is not as expected."
