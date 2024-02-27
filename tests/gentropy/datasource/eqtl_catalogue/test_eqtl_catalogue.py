"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from pyspark.sql import DataFrame

if TYPE_CHECKING:
    from gentropy.dataset.study_index import StudyIndex


def test_eqtl_catalogue_study_index_from_source(
    sample_eqtl_catalogue_studies: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        EqtlCatalogueStudyIndex.from_susie_results(
            sample_eqtl_catalogue_studies,
        ),
        StudyIndex,
    )
