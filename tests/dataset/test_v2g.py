"""Tests V2G dataset."""
from __future__ import annotations

from otg.dataset.v2g import V2G


def test_v2g_creation(mock_v2g: V2G) -> None:
    """Test v2g creation with mock data."""
    assert isinstance(mock_v2g, V2G)
