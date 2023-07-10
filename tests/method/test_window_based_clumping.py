"""Test window-based clumping."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.dataset.study_locus import StudyLocus
from otg.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from otg.dataset.summary_statistics import SummaryStatistics


def test_window_based_clump(mock_summary_statistics: SummaryStatistics) -> None:
    """Test window-based clumping."""
    assert isinstance(
        WindowBasedClumping.clump(mock_summary_statistics, 250_000), StudyLocus
    )
