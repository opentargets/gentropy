"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame

from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


@pytest.fixture
def processed_finemapping_df(
    sample_eqtl_catalogue_studies_metadata: DataFrame,
    sample_eqtl_catalogue_finemapping_credible_sets: DataFrame,
    sample_eqtl_catalogue_finemapping_lbf: DataFrame,
) -> DataFrame:
    """Return a DataFrame after joining credible sets and LBFs."""
    return EqtlCatalogueFinemapping.parse_susie_results(
        sample_eqtl_catalogue_finemapping_credible_sets,
        sample_eqtl_catalogue_finemapping_lbf,
        sample_eqtl_catalogue_studies_metadata,
    )


def test_parse_susie_results(
    sample_eqtl_catalogue_finemapping_credible_sets: DataFrame,
    sample_eqtl_catalogue_finemapping_lbf: DataFrame,
    sample_eqtl_catalogue_studies_metadata: DataFrame,
) -> None:
    """Test parsing SuSIE results."""
    assert isinstance(
        EqtlCatalogueFinemapping.parse_susie_results(
            sample_eqtl_catalogue_finemapping_credible_sets,
            sample_eqtl_catalogue_finemapping_lbf,
            sample_eqtl_catalogue_studies_metadata,
        ),
        DataFrame,
    )


def test_credsets_from_susie_results(processed_finemapping_df: DataFrame) -> None:
    """Test creating a study locus from SuSIE results."""
    assert isinstance(
        EqtlCatalogueFinemapping.from_susie_results(processed_finemapping_df),
        StudyLocus,
    )


def test_studies_from_susie_results(processed_finemapping_df: DataFrame) -> None:
    """Test creating a study index from SuSIE results."""
    assert isinstance(
        EqtlCatalogueStudyIndex.from_susie_results(processed_finemapping_df),
        StudyIndex,
    )
