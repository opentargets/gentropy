"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from pyspark.sql import DataFrame

from otg.dataset.study_index import StudyIndex
from otg.datasource.finngen.study_index import FinnGenStudyIndex


def test_finngen_study_index_from_source(
    sample_finngen_studies: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        FinnGenStudyIndex.from_source(
            sample_finngen_studies,
            "FINNGEN_R9_",
            "https://storage.googleapis.com/finngen-public-data-r9/summary_stats/finngen_R9_",
            ".gz",
        ),
        StudyIndex,
    )
