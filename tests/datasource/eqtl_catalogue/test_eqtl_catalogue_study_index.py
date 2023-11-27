"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

from pyspark.sql import DataFrame

from otg.dataset.study_index import StudyIndex
from otg.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


def test_eqtl_catalogue_study_index_from_source(
    sample_eqtl_catalogue_studies: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        EqtlCatalogueStudyIndex.from_source(
            sample_eqtl_catalogue_studies,
        ),
        StudyIndex,
    )
