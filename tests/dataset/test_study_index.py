"""Test study index dataset."""
from __future__ import annotations

from pyspark.sql import DataFrame

from otg.dataset.study_index import StudyIndex, StudyIndexFinnGen, StudyIndexGWASCatalog


def test_study_index_creation(mock_study_index: StudyIndex) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index, StudyIndex)


def test_study_index_gwas_catalog_creation(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index_gwas_catalog, StudyIndexGWASCatalog)


def test_study_index_finngen_creation(
    mock_study_index_finngen: StudyIndexFinnGen,
) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index_finngen, StudyIndexFinnGen)


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
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "nCases", "nControls", "nSamples"
    )
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_discovery_sample_sizes(),
        StudyIndexGWASCatalog,
    )


def test_parse_study_table(sample_gwas_catalog_studies: DataFrame) -> None:
    """Test parse study table."""
    assert isinstance(
        StudyIndexGWASCatalog._parse_study_table(sample_gwas_catalog_studies),
        StudyIndexGWASCatalog,
    )


def test_annotate_ancestry(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
    sample_gwas_catalog_ancestries_lut: DataFrame,
) -> None:
    """Test annotate ancestry of StudyIndexGWASCatalog."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "discoverySamples", "replicationSamples"
    )
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_ancestries(
            sample_gwas_catalog_ancestries_lut
        ),
        StudyIndexGWASCatalog,
    )


def test_annotate_sumstats(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
    sample_gwas_catalog_harmonised_sumstats: DataFrame,
) -> None:
    """Test annotate sumstats of StudyIndexGWASCatalog."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "summarystatsLocation"
    )
    assert isinstance(
        mock_study_index_gwas_catalog._annotate_sumstats_info(
            sample_gwas_catalog_harmonised_sumstats
        ),
        StudyIndexGWASCatalog,
    )


def test_study_index_from_source(
    sample_gwas_catalog_studies: DataFrame,
    sample_gwas_catalog_harmonised_sumstats: DataFrame,
    sample_gwas_catalog_ancestries_lut: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        StudyIndexGWASCatalog.from_source(
            sample_gwas_catalog_studies,
            sample_gwas_catalog_ancestries_lut,
            sample_gwas_catalog_harmonised_sumstats,
        ),
        StudyIndexGWASCatalog,
    )


def test_finngen_study_index_from_source(
    sample_finngen_studies: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        StudyIndexFinnGen.from_source(
            sample_finngen_studies,
            "FINNGEN_R8_",
            "https://storage.googleapis.com/finngen-public-data-r8/summary_stats/finngen_R8_",
            ".gz",
        ),
        StudyIndexFinnGen,
    )
