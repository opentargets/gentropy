"""Tests on LD index."""

from __future__ import annotations

from pyspark.sql import DataFrame

from gentropy.dataset.gene_index import GeneIndex


def test_gene_index_creation(mock_gene_index: GeneIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_gene_index, GeneIndex)


def test_gene_index_location_lut(mock_gene_index: GeneIndex) -> None:
    """Test gene index location lut."""
    assert isinstance(mock_gene_index.locations_lut(), DataFrame)


def test_gene_index_symbols_lut(mock_gene_index: GeneIndex) -> None:
    """Test gene index symbols lut."""
    assert isinstance(mock_gene_index.symbols_lut(), DataFrame)


def test_gene_index_filter_by_biotypes(mock_gene_index: GeneIndex) -> None:
    """Test gene index filter by biotypes."""
    assert isinstance(
        mock_gene_index.filter_by_biotypes(
            biotypes=["protein_coding", "3prime_overlapping_ncRNA", "antisense"]
        ),
        GeneIndex,
    )
