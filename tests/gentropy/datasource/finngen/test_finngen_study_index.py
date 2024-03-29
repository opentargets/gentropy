"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex
from pyspark.sql import SparkSession


def test_finngen_study_index_from_source(spark: SparkSession) -> None:
    """Test study index from source."""
    assert isinstance(FinnGenStudyIndex.from_source(spark), StudyIndex)
