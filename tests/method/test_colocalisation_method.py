"""Test colocalisation methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation import Coloc, ECaviar
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from gentropy.dataset.study_locus_overlap import StudyLocusOverlap


def test_coloc(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test coloc."""
    assert isinstance(Coloc.colocalise(mock_study_locus_overlap), Colocalisation)


def test_coloc_colocalise(
    spark: SparkSession,
) -> None:
    """Test COLOC with the sample dataset from R, transformed into StudyLocusOverlap object."""
    test_overlap = StudyLocusOverlap.from_parquet(
        spark, "tests/data_samples/coloc_test_data.snappy.parquet"
    )
    test_result = Coloc.colocalise(test_overlap)

    expected = spark.createDataFrame(
        [
            {
                "h0": 1.3769995397857477e-18,
                "h1": 2.937336451601565e-10,
                "h2": 8.593226431647826e-12,
                "h3": 8.338916748775843e-4,
                "h4": 0.9991661080227981,
            }
        ]
    )
    assert test_result.df.select("h0", "h1", "h2", "h3", "h4") == expected


def test_ecaviar(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test eCAVIAR."""
    assert isinstance(ECaviar.colocalise(mock_study_locus_overlap), Colocalisation)
