"""Tests on LD index."""
from __future__ import annotations

from otg.dataset.ld_index import LDIndex


def test_ld_index_creation(mock_ld_index: LDIndex) -> None:
    """Test ld index creation with mock ld index."""
    assert isinstance(mock_ld_index, LDIndex)


def test_annotate_index_intervals(mock_ld_index: LDIndex) -> None:
    """Test annotate index intervals."""
    assert isinstance(
        mock_ld_index.annotate_index_intervals(ld_radius=500_000), LDIndex
    )
