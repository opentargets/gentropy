"""Test window-based clumping."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from gentropy.dataset.summary_statistics import SummaryStatistics


def test_locus_breaker_return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test locus clumping."""
    assert isinstance(
        mock_summary_statistics.locus_breaker_clumping(),
        StudyLocus,
    )
