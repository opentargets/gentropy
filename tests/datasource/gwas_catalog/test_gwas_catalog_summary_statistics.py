"""Test GWAS Catalog summary statistics."""

from __future__ import annotations

from typing import TYPE_CHECKING

from otg.datasource.gwas_catalog.summary_statistics import GWASCatalogSummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def test_gwas_catalog_summary_statistics_from_gwas_harmonized_summary_stats(
    sample_gwas_catalog_harmonised_sumstats: DataFrame,
) -> None:
    """Test GWASCatalogSummaryStatistics creation with mock data."""
    assert isinstance(
        GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            sample_gwas_catalog_harmonised_sumstats, "GCST000000"
        ),
        GWASCatalogSummaryStatistics,
    )
