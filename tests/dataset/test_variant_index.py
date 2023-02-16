"""Tests variant index dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from otg.dataset.variant_annotation import VariantAnnotation


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)


def test_from_variant_annotation(mock_variant_annotation: VariantAnnotation) -> None:
    """Test variant index creation from variant annotation."""
    variant_index = VariantIndex.from_variant_annotation(mock_variant_annotation)
    assert isinstance(variant_index, VariantIndex)
