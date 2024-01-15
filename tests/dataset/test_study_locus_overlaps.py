"""Test colocalisation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.types as t
import pytest
from oxygen.dataset.study_locus import StudyLocus
from oxygen.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from oxygen.dataset.study_index import StudyIndex
    from pyspark.sql import SparkSession


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_study_locus_overlap_from_associations(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(mock_study_locus, mock_study_index)
    assert isinstance(overlaps, StudyLocusOverlap)


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # observed - input DataFrame representing gwas and nongwas data to find overlapping signals
            [
                {
                    "studyLocusId": 1,
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": 2,
                    "studyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": 3,
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "B",
                },
            ],
            # expected - output DataFrame with overlapping signals
            [
                {"leftStudyLocusId": 1, "rightStudyLocusId": 2, "chromosome": "1"},
            ],
        ),
    ],
)
def test_overlapping_peaks(
    spark: SparkSession, observed: list[dict[str, Any]], expected: list[dict[str, Any]]
) -> None:
    """Test overlapping signals between GWAS-GWAS and GWAS-Molecular trait to make sure that mQTLs are always on the right."""
    mock_schema = t.StructType(
        [
            t.StructField("studyLocusId", t.LongType()),
            t.StructField("studyType", t.StringType()),
            t.StructField("chromosome", t.StringType()),
            t.StructField("tagVariantId", t.StringType()),
        ]
    )
    expected_schema = t.StructType(
        [
            t.StructField("leftStudyLocusId", t.LongType()),
            t.StructField("rightStudyLocusId", t.LongType()),
            t.StructField("chromosome", t.StringType()),
        ]
    )
    observed_df = spark.createDataFrame(observed, mock_schema)
    result_df = StudyLocus._overlapping_peaks(observed_df)
    expected_df = spark.createDataFrame(expected, expected_schema)
    assert result_df.collect() == expected_df.collect()
