"""Tests on variant index generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    pass


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)
