"""Tests on helper functions that extract V2G assignments based on a variant's distance to a gene's TSS."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def mock_variant_df(spark: SparkSession) -> DataFrame:
    """Mock Dataframe for variants."""
    return spark.createDataFrame(
        [
            ("varOutsideSameChrom", 1, 3),
            ("varOutsideDiffChrom", 1, 3),
            ("varInside", 1, 8),
            ("varInside", 1, 12),
        ],
        ["variantId", "chromosome", "position"],
    )


@pytest.fixture(scope="module")
def mock_gene_df(spark: SparkSession) -> DataFrame:
    """Mock Dataframe for genes."""
    return spark.createDataFrame([("ENSGXXXXX", 1, 10)], ["id", "chromosome", "tss"])


class TestCalculateDistToGene:
    """Test the calculate_dist_to_gene util."""
