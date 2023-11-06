"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from pyspark.sql import DataFrame

from otg.dataset.summary_statistics import SummaryStatistics
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats


def test_finngen_summary_stats_from_source(
    sample_finngen_summary_stats: DataFrame,
) -> None:
    """Test summary statistics from source."""
    assert isinstance(
        FinnGenSummaryStats.from_source(sample_finngen_summary_stats),
        SummaryStatistics,
    )
