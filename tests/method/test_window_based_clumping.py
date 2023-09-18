"""Test window-based clumping."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from otg.dataset.study_locus import StudyLocus
from otg.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from otg.dataset.summary_statistics import SummaryStatistics


def test_window_based_clump__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    assert isinstance(
        WindowBasedClumping.clump_with_locus(mock_summary_statistics, 250_000),
        StudyLocus,
    )


def test_window_based_clump__correctness(
    sample_summary_satistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    clumped = sample_summary_satistics.window_based_clumping(250_000)

    # One semi index was found:
    assert clumped.df.count() == 1

    # Assert the variant found:
    assert (clumped.df.filter(f.col("variantId") == "18_12843138_T_C").count()) == 1


def test_window_based_clump_with_locus__correctness(
    sample_summary_satistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    clumped = sample_summary_satistics.window_based_clumping_with_locus(250_000)

    # Asserting the presence of locus key:
    assert "locus" in clumped.df.columns

    # One semi index was found:
    assert clumped.df.count() == 1

    # Assert the variant found:
    assert (clumped.df.filter(f.col("variantId") == "18_12843138_T_C").count()) == 1

    # Assert the number of variants in the locus:
    assert (clumped.df.select(f.explode_outer("locus").alias("loci")).count()) == 132
