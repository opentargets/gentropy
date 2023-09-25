"""Test StudyIndexGWASCatalog."""

from __future__ import annotations

from pyspark.sql import DataFrame

from otg.datasource.gwas_catalog.study_index import GWASCatalogStudyIndex


def test_annotate_discovery_sample_sizes(
    mock_study_index_gwas_catalog: GWASCatalogStudyIndex,
) -> None:
    """Test annotate discovery sample sizes."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "nCases", "nControls", "nSamples"
    )
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_discovery_sample_sizes(),
        GWASCatalogStudyIndex,
    )


def test_parse_study_table(sample_gwas_catalog_studies: DataFrame) -> None:
    """Test parse study table."""
    assert isinstance(
        GWASCatalogStudyIndex._parse_study_table(sample_gwas_catalog_studies),
        GWASCatalogStudyIndex,
    )


def test_annotate_ancestry(
    mock_study_index_gwas_catalog: GWASCatalogStudyIndex,
    sample_gwas_catalog_ancestries_lut: DataFrame,
) -> None:
    """Test annotate ancestry of GWASCatalogStudyIndex."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "discoverySamples", "replicationSamples"
    )
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_ancestries(
            sample_gwas_catalog_ancestries_lut
        ),
        GWASCatalogStudyIndex,
    )


def test_annotate_sumstats(
    mock_study_index_gwas_catalog: GWASCatalogStudyIndex,
    sample_gwas_catalog_harmonised_sumstats: DataFrame,
) -> None:
    """Test annotate sumstats of GWASCatalogStudyIndex."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "summarystatsLocation"
    )
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_sumstats_info(
            sample_gwas_catalog_harmonised_sumstats
        ),
        GWASCatalogStudyIndex,
    )


def test_study_index_from_source(
    sample_gwas_catalog_studies: DataFrame,
    sample_gwas_catalog_harmonised_sumstats: DataFrame,
    sample_gwas_catalog_ancestries_lut: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        GWASCatalogStudyIndex.from_source(
            sample_gwas_catalog_studies,
            sample_gwas_catalog_ancestries_lut,
            sample_gwas_catalog_harmonised_sumstats,
        ),
        GWASCatalogStudyIndex,
    )
