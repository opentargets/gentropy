"""Tests GWAS Catalog study splitter."""
from __future__ import annotations

from otg.common.gwas_catalog_splitter import GWASCatalogSplitter
from otg.dataset.study_index import StudyIndexGWASCatalog
from otg.dataset.study_locus import StudyLocusGWASCatalog


def test_gwas_catalog_splitter_split(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
) -> None:
    """Test StudyIndexGWASCatalog, StudyLocusGWASCatalog creation with mock data."""
    d1, d2 = GWASCatalogSplitter.split(
        mock_study_index_gwas_catalog, mock_study_locus_gwas_catalog
    )

    assert isinstance(d1, StudyIndexGWASCatalog)
    assert isinstance(d2, StudyLocusGWASCatalog)
