"""Test colocalisation methods."""

from __future__ import annotations

from typing import Any

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation import Coloc, ECaviar
from pyspark.sql import functions as f


def test_coloc(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test coloc."""
    assert isinstance(Coloc.colocalise(mock_study_locus_overlap), Colocalisation)


def test_coloc_colocalise(
    sample_data_for_coloc: list[Any],
    threshold: float = 1e-4,
) -> None:
    """Compare COLOC results with R implementation, using provided sample dataset from R package (StudyLocusOverlap)."""
    test_overlap_df = sample_data_for_coloc[0]
    test_overlap = StudyLocusOverlap(
        _df=test_overlap_df, _schema=StudyLocusOverlap.get_schema()
    )
    test_result = Coloc.colocalise(test_overlap)
    expected = sample_data_for_coloc[1]
    difference = test_result.df.select("h0", "h1", "h2", "h3", "h4").subtract(expected)
    for col in difference.columns:
        assert difference.filter(f.abs(f.col(col)) > threshold).count() == 0


def test_single_snp_coloc(
    sample_data_for_coloc: list[Any],
    threshold: float = 1e-5,
) -> None:
    """Test edge case of coloc where only one causal SNP is present in the StudyLocusOverlap."""
    test_overlap_df = sample_data_for_coloc[2]
    test_overlap = StudyLocusOverlap(
        _df=test_overlap_df.select(
            "leftStudyLocusId",
            "rightStudyLocusId",
            "chromosome",
            "tagVariantId",
            f.struct(f.col("left_logBF"), f.col("right_logBF")).alias("statistics"),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )
    test_result = Coloc.colocalise(test_overlap)
    expected = sample_data_for_coloc[3]
    difference = test_result.df.select("h0", "h1", "h2", "h3", "h4").subtract(expected)
    for col in difference.columns:
        assert difference.filter(f.abs(f.col(col)) > threshold).count() == 0


def test_ecaviar(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test eCAVIAR."""
    assert isinstance(ECaviar.colocalise(mock_study_locus_overlap), Colocalisation)
