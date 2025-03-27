"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

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


class TestEqtlCatalogueStudyLocus:
    """Test the correctness of the study locus dataset from eQTL Catalogue."""

    @pytest.fixture(autouse=True)
    def _setup(self, processed_finemapping_df: DataFrame) -> DataFrame:
        """Set up the test."""
        self.study_locus = EqtlCatalogueFinemapping.from_susie_results(
            processed_finemapping_df
        )

    def test_credsets_from_susie_results(self: TestEqtlCatalogueStudyLocus) -> None:
        """Test creating a study locus from SuSIE results."""
        assert isinstance(self.study_locus, StudyLocus)

    def test_locus_uniqueness(self: TestEqtlCatalogueStudyLocus) -> None:
        """Test the uniqueness of the locus."""
        find_discrepancies = self.study_locus.df.select(
            f.size("locus").alias("locus_size"),
            f.size(f.array_distinct("locus")).alias("locus_distinct_size"),
        ).filter(f.col("locus_size") != f.col("locus_distinct_size"))
        assert find_discrepancies.count() == 0


def test_studies_from_susie_results(processed_finemapping_df: DataFrame) -> None:
    """Test creating a study index from SuSIE results."""
    assert isinstance(
        EqtlCatalogueStudyIndex.from_susie_results(processed_finemapping_df),
        StudyIndex,
    )


def test_study_identifier_sanitisation(processed_finemapping_df: DataFrame) -> None:
    """Test that the study identifiers are sanitised."""
    replaced_characters = r"[\+\:]"

    # Assert the presence of the characters that need to be replaced in the source data:
    assert (
        processed_finemapping_df.filter(
            f.col("molecular_trait_id").rlike(replaced_characters)
        ).count()
        > 0
    )

    # Assert the absence pf the characters that need to be replaced in the study index:
    assert (
        EqtlCatalogueStudyIndex.from_susie_results(processed_finemapping_df)
        .df.filter(f.col("studyId").rlike(replaced_characters))
        .count()
        == 0
    )
