"""Test study index dataset."""
from __future__ import annotations

from otg.dataset.study_locus import StudyLocus
from otg.dataset.summary_statistics import SummaryStatistics


def test_summary_statistics__creation(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_summary_statistics, SummaryStatistics)


def test_summary_statistics__pval_filter__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if the p-value filter indeed returns summary statistics object."""
    pval_threshold = 5e-3
    assert isinstance(
        mock_summary_statistics.pvalue_filter(pval_threshold), SummaryStatistics
    )


def test_summary_statistics__calculate_confidence_interval__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if the confidence interval calculation indeed returns summary statistics object."""
    assert isinstance(
        mock_summary_statistics.calculate_confidence_interval(), SummaryStatistics
    )


def test_summary_statistics__calculate_confidence_interval__new_columns(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if the confidence interval calculation adds the right column."""
    columns = mock_summary_statistics.calculate_confidence_interval().df.columns

    # These two columns are computed by the tested function:
    assert "betaConfidenceIntervalLower" in columns
    assert "betaConfidenceIntervalUpper" in columns


def test_summary_statistics__window_based_clumping__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if the window-based clumping indeed returns study locus object."""
    assert isinstance(
        mock_summary_statistics.window_based_clumping(250_000), StudyLocus
    )
