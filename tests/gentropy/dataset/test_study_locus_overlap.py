"""Test study locus overlap dataset."""

from __future__ import annotations

from pyspark.sql import SparkSession

from gentropy.dataset.study_locus_overlap import StudyLocusOverlap


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test study locus overlap creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_convert_to_square_matrix(spark: SparkSession) -> None:
    """Test _convert_to_square_matrix."""
    mock_sl_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            [
                ("1", "2", "eqtl", "CHROM1", "variant2", None),
            ],
            StudyLocusOverlap.get_schema(),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "2", "eqtl", "CHROM1", "variant2", None),
            ("2", "1", "eqtl", "CHROM1", "variant2", None),
        ],
        StudyLocusOverlap.get_schema(),
    )
    observed_df = mock_sl_overlap._convert_to_square_matrix().df

    assert observed_df.collect() == expected_df.collect()
