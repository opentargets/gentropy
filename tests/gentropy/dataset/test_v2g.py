"""Tests V2G dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.v2g import V2G

if TYPE_CHECKING:
    from gentropy.dataset.gene_index import GeneIndex


def test_v2g_creation(mock_v2g: V2G) -> None:
    """Test v2g creation with mock data."""
    assert isinstance(mock_v2g, V2G)


def test_v2g_filter_by_genes(mock_v2g: V2G, mock_gene_index: GeneIndex) -> None:
    """Test v2g filter by genes."""
    assert isinstance(
        mock_v2g.filter_by_genes(mock_gene_index),
        V2G,
    )
