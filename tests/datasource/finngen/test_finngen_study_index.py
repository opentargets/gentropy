"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from pyspark.sql import SparkSession

from otg.dataset.study_index import StudyIndex
from otg.datasource.finngen.study_index import FinnGenStudyIndex


def test_finngen_study_index_from_source(spark: SparkSession) -> None:
    """Test study index from source."""
    assert isinstance(FinnGenStudyIndex.from_source(spark), StudyIndex)
