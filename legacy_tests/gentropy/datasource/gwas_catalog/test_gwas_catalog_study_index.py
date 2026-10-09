"""Test GWASCatalogStudyIndex."""

from __future__ import annotations

from pyspark.sql import DataFrame

from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalog,
    StudyIndexGWASCatalogParser,
)


def test_annotate_discovery_sample_sizes(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
) -> None:
    """Test annotate discovery sample sizes."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "nCases", "nControls", "nSamples"
    )
    assert isinstance(
        mock_study_index_gwas_catalog.annotate_discovery_sample_sizes(),
        StudyIndexGWASCatalog,
    )


def test_parse_study_table(sample_gwas_catalog_studies: DataFrame) -> None:
    """Test parse study table."""
    assert isinstance(
        StudyIndexGWASCatalogParser._parse_study_table(sample_gwas_catalog_studies),
        StudyIndexGWASCatalog,
    )


def test_study_index_from_source(
    sample_gwas_catalog_studies: DataFrame,
    sample_gwas_catalog_ancestries_lut: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        StudyIndexGWASCatalogParser.from_source(
            sample_gwas_catalog_studies,
            sample_gwas_catalog_ancestries_lut,
        ),
        StudyIndexGWASCatalog,
    )
