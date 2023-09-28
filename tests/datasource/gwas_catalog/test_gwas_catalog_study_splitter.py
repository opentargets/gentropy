"""Tests GWAS Catalog study splitter."""
from __future__ import annotations

from otg.datasource.gwas_catalog.associations import GWASCatalogAssociations
from otg.datasource.gwas_catalog.study_index import GWASCatalogStudyIndex
from otg.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter


def test_gwas_catalog_splitter_split(
    mock_study_index_gwas_catalog: GWASCatalogStudyIndex,
    mock_study_locus_gwas_catalog: GWASCatalogAssociations,
) -> None:
    """Test GWASCatalogStudyIndex, GWASCatalogAssociations creation with mock data."""
    d1, d2 = GWASCatalogStudySplitter.split(
        mock_study_index_gwas_catalog, mock_study_locus_gwas_catalog
    )

    assert isinstance(d1, GWASCatalogStudyIndex)
    assert isinstance(d2, GWASCatalogAssociations)
