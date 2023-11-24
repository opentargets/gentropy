"""Test GWAS Catalog summary statistics."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from otg.dataset.summary_statistics import SummaryStatistics
from otg.datasource.gwas_catalog.summary_statistics import GWASCatalogSummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def gwas_catalog_summary_statistics__new_format(
    spark: SparkSession,
) -> GWASCatalogSummaryStatistics:
    """Test GWASCatalogSummaryStatistics creation with mock data."""
    return GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
        spark, "tests/data_samples/new_format_GCST90293086.h.tsv.gz"
    )


def test_return_type(
    gwas_catalog_summary_statistics__new_format: SummaryStatistics,
) -> None:
    """Testing return type."""
    assert isinstance(gwas_catalog_summary_statistics__new_format, SummaryStatistics)


def test_p_value_parsed_correctly(
    gwas_catalog_summary_statistics__new_format: SummaryStatistics,
) -> None:
    """Testing parsed p-value."""
    assert (
        gwas_catalog_summary_statistics__new_format.df.filter(
            f.col("pValueMantissa").isNotNull()
        ).count()
        > 1
    )


def test_effect_parsed_correctly(
    gwas_catalog_summary_statistics__new_format: SummaryStatistics,
) -> None:
    """Testing properly parsed effect."""
    assert (
        gwas_catalog_summary_statistics__new_format.df.filter(
            f.col("beta").isNotNull()
        ).count()
        > 1
    )


def test_study_id(
    gwas_catalog_summary_statistics__new_format: SummaryStatistics,
) -> None:
    """Testing properly parsed effect."""
    assert (
        gwas_catalog_summary_statistics__new_format.df.filter(
            f.col("studyId").startswith("GCST")
        ).count()
        == gwas_catalog_summary_statistics__new_format.df.count()
    )
