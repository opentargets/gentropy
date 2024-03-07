"""Tests on LD index."""

from __future__ import annotations

from gentropy.dataset.ld_index import LDIndex


def test_ld_index_creation(mock_ld_index: LDIndex) -> None:
    """Test ld index creation with mock ld index."""
    assert isinstance(mock_ld_index, LDIndex)
