"""Test study locus dataset."""
from __future__ import annotations

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import CredibleInterval, StudyLocus, StudyLocusOverlap


def test_study_locus_creation(mock_study_locus: StudyLocus) -> None:
    """Test study locus creation with mock data."""
    assert isinstance(mock_study_locus, StudyLocus)


def test_study_locus_overlaps(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test study locus overlaps."""
    assert isinstance(
        mock_study_locus.find_overlaps(mock_study_index), StudyLocusOverlap
    )


def test_credible_set(mock_study_locus: StudyLocus) -> None:
    """Test credible interval."""
    assert isinstance(mock_study_locus.credible_set(CredibleInterval.IS95), StudyLocus)


def test_unique_lead_tag_variants(mock_study_locus: StudyLocus) -> None:
    """Test unique lead tag variants."""
    assert isinstance(mock_study_locus.unique_lead_tag_variants(), DataFrame)


def test_neglog_pvalue(mock_study_locus: StudyLocus) -> None:
    """Test neglog pvalue."""
    assert isinstance(mock_study_locus.neglog_pvalue(), Column)


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test clump."""
    assert isinstance(mock_study_locus.clump(), StudyLocus)


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # Simple case
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [{"variantId": "tagVariantA", "posteriorProbability": 1.0}],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    [
                        {
                            "variantId": "tagVariantA",
                            "posteriorProbability": 1.0,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        }
                    ],
                )
            ],
        ),
        (
            # Unordered credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadA",
                    [
                        {"variantId": "tagVariantA", "posteriorProbability": 0.44},
                        {"variantId": "tagVariantB", "posteriorProbability": 0.01},
                        {"variantId": "tagVariantC", "posteriorProbability": 0.04},
                        {"variantId": "tagVariantD", "posteriorProbability": 0.01},
                        {"variantId": "tagVariantE", "posteriorProbability": 0.5},
                    ],
                )
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadA",
                    [
                        {
                            "variantId": "tagVariantE",
                            "posteriorProbability": 0.5,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "variantId": "tagVariantA",
                            "posteriorProbability": 0.44,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "variantId": "tagVariantC",
                            "posteriorProbability": 0.04,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                    ],
                )
            ],
        ),
        (
            # Null credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                )
            ],
        ),
        (
            # Empty credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    [],
                )
            ],
        ),
    ],
)
def test_annotate_credible_sets(
    spark: SparkSession, observed: list, expected: list
) -> None:
    """Test annotate_credible_sets."""
    schema = StructType(
        [
            StructField("studyLocusId", LongType(), True),
            StructField("studyId", StringType(), True),
            StructField("variantId", StringType(), True),
            StructField(
                "locus",
                ArrayType(
                    StructType(
                        [
                            StructField("variantId", StringType(), True),
                            StructField("posteriorProbability", DoubleType(), True),
                            StructField("is95CredibleSet", BooleanType(), True),
                            StructField("is99CredibleSet", BooleanType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    data_sl = StudyLocus(
        _df=spark.createDataFrame(observed, schema), _schema=StudyLocus.get_schema()
    ).annotate_credible_sets()
    expected_sl = StudyLocus(
        _df=spark.createDataFrame(expected, schema), _schema=StudyLocus.get_schema()
    )
    assert data_sl.annotate_credible_sets().df.collect() == expected_sl.df.collect()
