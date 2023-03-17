"""Test study index dataset."""
from __future__ import annotations

from pyspark.sql import DataFrame

from otg.dataset.study_index import StudyIndex, StudyIndexGWASCatalog


def test_study_index_creation(mock_study_index: StudyIndex) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index, StudyIndex)


def test_study_index_gwas_catalog_creation(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index_gwas_catalog, StudyIndexGWASCatalog)


def test_study_index_type_lut(mock_study_index: StudyIndex) -> None:
    """Test study index type lut."""
    assert isinstance(mock_study_index.study_type_lut(), DataFrame)


def test_study_gnomad_ancestry_sample_sizes(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
) -> None:
    """Test study index gnomad ancestry sample sizes."""
    assert isinstance(
        mock_study_index_gwas_catalog.get_gnomad_ancestry_sample_sizes(), DataFrame
    )


def test_annotate_discovery_sample_sizes(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
) -> None:
    """Test annotate discovery sample sizes."""
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_discovery_sample_sizes(),
        StudyIndexGWASCatalog,
    )
