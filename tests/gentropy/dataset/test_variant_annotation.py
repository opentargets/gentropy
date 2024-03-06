"""Tests variant annotation dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gentropy.dataset.gene_index import GeneIndex

from gentropy.dataset.v2g import V2G
from gentropy.dataset.variant_annotation import VariantAnnotation


def test_variant_index_creation(mock_variant_annotation: VariantAnnotation) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_annotation, VariantAnnotation)


def test_get_plof_v2g(
    mock_variant_annotation: VariantAnnotation, mock_gene_index: GeneIndex
) -> None:
    """Test get_plof_v2g with mock variant annotation."""
    assert isinstance(mock_variant_annotation.get_plof_v2g(mock_gene_index), V2G)


def test_get_distance_to_tss(
    mock_variant_annotation: VariantAnnotation, mock_gene_index: GeneIndex
) -> None:
    """Test get_distance_to_tss with mock variant annotation."""
    assert isinstance(mock_variant_annotation.get_distance_to_tss(mock_gene_index), V2G)
