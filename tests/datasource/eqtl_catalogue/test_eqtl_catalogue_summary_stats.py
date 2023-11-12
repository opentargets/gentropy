"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

from pyspark.sql import DataFrame

from otg.dataset.summary_statistics import SummaryStatistics
from otg.datasource.eqtl_catalogue.summary_stats import EqtlCatalogueSummaryStats


def test_eqtl_catalogue_summary_stats_from_source(
    sample_eqtl_catalogue_summary_stats: DataFrame,
) -> None:
    """Test summary statistics from source."""
    assert isinstance(
        EqtlCatalogueSummaryStats.from_source(sample_eqtl_catalogue_summary_stats),
        SummaryStatistics,
    )
