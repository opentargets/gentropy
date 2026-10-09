"""Tests for study index dataset from UK Biobank."""

from __future__ import annotations

from pyspark.sql import DataFrame

from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.ukbiobank.study_index import UKBiobankStudyIndex


def test_ukbiobank_study_index_from_source(
    sample_ukbiobank_studies: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        UKBiobankStudyIndex.from_source(
            sample_ukbiobank_studies,
        ),
        StudyIndex,
    )
