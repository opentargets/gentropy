"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

from oxygen.dataset.summary_statistics import SummaryStatistics
from oxygen.datasource.eqtl_catalogue.summary_stats import EqtlCatalogueSummaryStats
from pyspark.sql import DataFrame


def test_eqtl_catalogue_summary_stats_from_source(
    sample_eqtl_catalogue_summary_stats: DataFrame,
) -> None:
    """Test summary statistics from source."""
    assert isinstance(
        EqtlCatalogueSummaryStats.from_source(sample_eqtl_catalogue_summary_stats),
        SummaryStatistics,
    )
