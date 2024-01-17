"""Tests on LD index."""
from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.v2g import V2G

if TYPE_CHECKING:
    from gentropy.dataset.intervals import Intervals
    from gentropy.dataset.variant_index import VariantIndex


def test_interval_v2g_creation(
    mock_intervals: Intervals, mock_variant_index: VariantIndex
) -> None:
    """Test creation of V2G from intervals."""
    assert isinstance(mock_intervals.v2g(mock_variant_index), V2G)
