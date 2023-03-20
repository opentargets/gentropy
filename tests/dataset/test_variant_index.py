"""Tests variant index dataset."""
from __future__ import annotations

from otg.dataset.variant_index import VariantIndex


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)
