"""Test window-based clumping."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.window_based_clumping import WindowBasedClumping
from pyspark.sql import functions as f

if TYPE_CHECKING:
    from gentropy.dataset.summary_statistics import SummaryStatistics


def test_window_based_clump__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    assert isinstance(
        WindowBasedClumping.clump(mock_summary_statistics, distance=250_000),
        StudyLocus,
    )
    assert isinstance(
        WindowBasedClumping.clump(
            mock_summary_statistics,
            distance=250_000,
            collect_locus=True,
            collect_locus_distance=250_000,
        ),
        StudyLocus,
    )


def test_window_based_clump__correctness(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    clumped = sample_summary_statistics.window_based_clumping(250_000)

    # One semi index was found:
    assert clumped.df.count() == 1

    # Assert the variant found:
    assert (clumped.df.filter(f.col("variantId") == "18_12843138_T_C").count()) == 1


def test_window_based_clump_with_locus__correctness(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    clumped = sample_summary_statistics.window_based_clumping(
        distance=250_000, collect_locus_distance=250_000, collect_locus=True
    )

    # Asserting the presence of locus key:
    assert "locus" in clumped.df.columns

    # One semi index was found:
    assert clumped.df.count() == 1

    # Assert the variant found:
    assert (clumped.df.filter(f.col("variantId") == "18_12843138_T_C").count()) == 1

    # Assert the number of variants in the locus:
    assert (clumped.df.select(f.explode_outer("locus").alias("loci")).count()) == 218
