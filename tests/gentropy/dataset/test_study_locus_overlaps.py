"""Test colocalisation dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest

from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_study_locus_overlap_from_associations(mock_study_locus: StudyLocus) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(mock_study_locus)
    assert isinstance(overlaps, StudyLocusOverlap)


@pytest.mark.parametrize(
    ("observed", "intrastudy", "expected"),
    [
        (
            # observed - input DataFrame representing gwas and nongwas data to find overlapping signals
            [
                {
                    "studyLocusId": "1",
                    "studyId": "A",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": "2",
                    "studyId": "B",
                    "studyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": "3",
                    "studyId": "C",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "B",
                },
            ],
            # intrastudy - bool of whether or not to use inter-study or intra-study logic
            False,
            # expected - output DataFrame with overlapping signals
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                },
            ],
        ),
        (
            # observed - input DataFrame representing intra-study data to find overlapping signals in the same study
            [
                {
                    "studyLocusId": "1",
                    "studyId": "A",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "region": "X",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": "2",
                    "studyId": "A",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "region": "Y",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": "3",
                    "studyId": "B",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "region": "X",
                    "tagVariantId": "A",
                },
            ],
            # intrastudy - bool of whether or not to use inter-study or intra-study logic
            True,
            # expected - output DataFrame with overlapping signals
            [
                {
                    "leftStudyLocusId": "2",
                    "rightStudyLocusId": "1",
                    "rightStudyType": "gwas",
                    "chromosome": "1",
                }
            ],
        ),
    ],
)
def test_overlapping_peaks(
    spark: SparkSession,
    observed: list[dict[str, Any]],
    intrastudy: bool,
    expected: list[dict[str, Any]],
) -> None:
    """Test overlapping signals between GWAS-GWAS and GWAS-Molecular trait to make sure that mQTLs are always on the right."""
    mock_schema = t.StructType(
        [
            t.StructField("studyLocusId", t.StringType()),
            t.StructField("studyId", t.StringType()),
            t.StructField("studyType", t.StringType()),
            t.StructField("chromosome", t.StringType()),
            t.StructField("region", t.StringType()),
            t.StructField("tagVariantId", t.StringType()),
        ]
    )
    expected_schema = t.StructType(
        [
            t.StructField("leftStudyLocusId", t.StringType()),
            t.StructField("rightStudyLocusId", t.StringType()),
            t.StructField("rightStudyType", t.StringType()),
            t.StructField("chromosome", t.StringType()),
        ]
    )
    observed_df = spark.createDataFrame(observed, mock_schema)
    result_df = StudyLocus._overlapping_peaks(observed_df, intrastudy)
    expected_df = spark.createDataFrame(expected, expected_schema)
    assert result_df.collect() == expected_df.collect()


class TestStudyLocusOverlap:
    """Test the overlapping of StudyLocus dataset."""

    @pytest.fixture(autouse=True)
    def setup(
        self: TestStudyLocusOverlap, study_locus_sample_for_colocalisation: StudyLocus
    ) -> None:
        """Get sample dataset."""
        # Store imput dataset:
        self.study_locus = study_locus_sample_for_colocalisation

        # Call locus overlap:
        self.overlaps = study_locus_sample_for_colocalisation.find_overlaps()

    def test_coloc_return_type(self: TestStudyLocusOverlap) -> None:
        """Test get_schema."""
        assert isinstance(self.overlaps, StudyLocusOverlap)

    def test_coloc_not_null(self: TestStudyLocusOverlap) -> None:
        """Test get_schema."""
        assert self.overlaps.df.count() != 0

    def test_coloc_study_type_not_null(self: TestStudyLocusOverlap) -> None:
        """Test get_schema."""
        assert self.overlaps.filter(f.col("rightStudyType").isNull()).df.count() == 0
