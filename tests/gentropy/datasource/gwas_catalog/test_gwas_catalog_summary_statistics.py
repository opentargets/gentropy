"""Test GWAS Catalog summary statistics."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest
from pyspark.sql import Row

from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.gwas_catalog.summary_statistics import (
    GWASCatalogSummaryStatistics,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pytest import FixtureRequest


class TestGWASCatalogSummaryStatistics:
    """Test suite for GWAS Catalog summary stats ingestion."""

    @pytest.fixture(scope="class")
    def gwas_catalog_summary_statistics__new_format(
        self: TestGWASCatalogSummaryStatistics,
        spark: SparkSession,
    ) -> GWASCatalogSummaryStatistics:
        """Test GWASCatalogSummaryStatistics creation with mock data."""
        return GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            spark, "tests/gentropy/data_samples/new_format_GCST90293086.h.tsv.gz"
        )

    @pytest.fixture(scope="class")
    def gwas_catalog_summary_statistics__old_format(
        self: TestGWASCatalogSummaryStatistics,
        spark: SparkSession,
    ) -> GWASCatalogSummaryStatistics:
        """Test GWASCatalogSummaryStatistics creation with mock data."""
        return GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            spark, "tests/gentropy/data_samples/old_format_GCST006090.h.tsv.gz"
        )

    @pytest.fixture(scope="class")
    def test_dataset_instance(
        self: TestGWASCatalogSummaryStatistics, request: FixtureRequest
    ) -> GWASCatalogSummaryStatistics:
        """Meta fixture to return the value of any requested fixture."""
        return request.getfixturevalue(request.param)

    @pytest.mark.parametrize(
        "test_dataset_instance",
        [
            "gwas_catalog_summary_statistics__old_format",
            "gwas_catalog_summary_statistics__new_format",
        ],
        indirect=True,
    )
    def test_return_type(
        self: TestGWASCatalogSummaryStatistics,
        test_dataset_instance: SummaryStatistics,
    ) -> None:
        """Testing return type."""
        assert isinstance(test_dataset_instance, SummaryStatistics)

    @pytest.mark.parametrize(
        "test_dataset_instance",
        [
            "gwas_catalog_summary_statistics__old_format",
            "gwas_catalog_summary_statistics__new_format",
        ],
        indirect=True,
    )
    def test_p_value_parsed_correctly(
        self: TestGWASCatalogSummaryStatistics,
        test_dataset_instance: SummaryStatistics,
    ) -> None:
        """Testing parsed p-value."""
        assert (
            test_dataset_instance.df.filter(f.col("pValueMantissa").isNotNull()).count()
            > 1
        )

    @pytest.mark.parametrize(
        "test_dataset_instance",
        [
            "gwas_catalog_summary_statistics__old_format",
            "gwas_catalog_summary_statistics__new_format",
        ],
        indirect=True,
    )
    def test_effect_parsed_correctly(
        self: TestGWASCatalogSummaryStatistics,
        test_dataset_instance: SummaryStatistics,
    ) -> None:
        """Testing properly parsed effect."""
        assert test_dataset_instance.df.filter(f.col("beta").isNotNull()).count() > 1

    @pytest.mark.parametrize(
        "test_dataset_instance",
        [
            "gwas_catalog_summary_statistics__old_format",
            "gwas_catalog_summary_statistics__new_format",
        ],
        indirect=True,
    )
    def test_study_id(
        self: TestGWASCatalogSummaryStatistics,
        test_dataset_instance: SummaryStatistics,
    ) -> None:
        """Testing properly parsed effect."""
        assert (
            test_dataset_instance.df.filter(f.col("studyId").startswith("GCST")).count()
            == test_dataset_instance.df.count()
        )

    def test_rescue_standard_error(
        self: TestGWASCatalogSummaryStatistics, spark: SparkSession
    ) -> None:
        """Test rescue standard error."""
        # The new format has standard error, but the old format does not.
        test_dataset_path = "tests/gentropy/data_samples/empty_stderr_GCST01.h.tsv"
        sumstat = GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            spark, test_dataset_path
        )
        # case 1 - position 1026830 - skipped row, since we can not recompute the standard error without ci
        data = sumstat.df.filter(f.col("position") == 1026830).filter(
            f.col("standardError").isNull()
        )
        assert data.count() == 1

        # case 2 - position 1026831 - skipped, since beta is None, standard error is None
        data = sumstat.df.filter(f.col("position") == 1026831)
        assert data.count() == 0

        # case 3 - position 1026832 - standard error is retained from the source (0.2)
        data = (
            sumstat.df.filter(f.col("position") == 1026832)
            .filter(f.col("standardError").isNotNull())
            .select(f.round(f.col("standardError"), 1))
        )
        assert data.collect() == [Row(standardError=0.2)]

        # case 4 - position 1026833 - standard error is calculated from ci
        data = sumstat.df.filter(f.col("position") == 1026833)
        assert data.count() == 1
