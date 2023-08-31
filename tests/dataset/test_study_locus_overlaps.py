"""Test colocalisation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.types as t
import pytest

from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap, StudyLocusOverlapMethod

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from otg.dataset.study_index import StudyIndex


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_study_locus_overlap_from_associations(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(
        StudyLocusOverlapMethod.LD, mock_study_locus, mock_study_index
    )
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
                {"left_studyLocusId": 1, "right_studyLocusId": 2, "chromosome": "1"},
            ],
        ),
    ],
)
def test_overlapping_peaks(spark: SparkSession, observed: list, expected: list) -> None:
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
            t.StructField("left_studyLocusId", t.LongType()),
            t.StructField("right_studyLocusId", t.LongType()),
            t.StructField("chromosome", t.StringType()),
        ]
    )
    observed_df = spark.createDataFrame(observed, mock_schema)
    result_df = StudyLocus._overlapping_peaks(observed_df)
    expected_df = spark.createDataFrame(expected, expected_schema)
    assert result_df.collect() == expected_df.collect()
