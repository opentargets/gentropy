"""Test GWAS Catalog summary statistics."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.gwas_catalog.summary_statistics import (
    GWASCatalogSummaryStatistics,
)

if TYPE_CHECKING:
    from pathlib import Path

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

    @pytest.mark.parametrize(
        ["position", "expStdErr"],
        [
            pytest.param(
                1026830, 0.14826, id="Do not rescue, when se (1.48260) exists"
            ),
            pytest.param(
                1026831, 0.18344, id="Rescue se from p-value (5e-8) and beta (1.0)"
            ),
            pytest.param(
                1026832,
                0.18344,
                id="Rescue se from p-value (5e-8) and OR (euler's number)",
            ),
        ],
    )
    def test_rescue_standard_error(
        self: TestGWASCatalogSummaryStatistics,
        spark: SparkSession,
        position: int,
        expStdErr: float,
    ) -> None:
        """Test rescue standard error from GWAS summary statistics based on what we have.

        The test above assumes that the p-value is always 0.5 and beta (if present) is 1.0.
        """
        # The new format has standard error, but the old format does not.
        test_dataset_path = "tests/gentropy/data_samples/empty_stderr_GCST01.h.tsv"
        sumstat = GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            spark, test_dataset_path
        )

        def get_stderr(pos: int) -> float | None:
            """Extract standard error from a row."""
            rows = sumstat.df.filter(f.col("position") == pos).collect()
            assert len(rows) == 1, "Expected exactly one row for position"
            if not rows:
                return None
            return rows[0]["standardError"]

        assert get_stderr(position) == pytest.approx(expStdErr, abs=1e-5)

    def test_chromosome_normalised(
        self: TestGWASCatalogSummaryStatistics,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """Test that numeric sex chromosomes are coded as X and Y in the chromosome and the variant id."""
        sumstats_path = tmp_path / "GCST02.h.tsv"
        sumstats_path.write_text(
            "\n".join(
                [
                    "chromosome\tbase_pair_location\teffect_allele\tother_allele\tbeta\tstandard_error\tp_value",
                    "1\t100\tA\tG\t0.1\t0.01\t0.001",
                    "23\t200\tA\tG\t0.1\t0.01\t0.001",
                    "24\t23359151\tA\tG\t0.1\t0.01\t0.001",
                ]
            )
        )
        sumstat = GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            spark, str(sumstats_path)
        )

        observed = {
            (row["chromosome"], row["variantId"])
            for row in sumstat.df.select("chromosome", "variantId").collect()
        }
        assert observed == {
            ("1", "1_100_G_A"),
            ("X", "X_200_G_A"),
            ("Y", "Y_23359151_G_A"),
        }
