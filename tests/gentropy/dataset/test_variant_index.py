"""Tests on variant index generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.v2g import V2G
from gentropy.dataset.variant_index import VariantIndex
from pyspark.sql import functions as f
from pyspark.sql import types as t

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)


def test_get_plof_v2g(
    mock_variant_index: VariantIndex, mock_gene_index: GeneIndex
) -> None:
    """Test get_plof_v2g with mock variant annotation."""
    assert isinstance(mock_variant_index.get_plof_v2g(mock_gene_index), V2G)


def test_get_distance_to_tss(
    mock_variant_index: VariantIndex, mock_gene_index: GeneIndex
) -> None:
    """Test get_distance_to_tss with mock variant annotation."""
    assert isinstance(mock_variant_index.get_distance_to_tss(mock_gene_index), V2G)


class TestVariantIndex:
    """Collection of tests around the functionality and shape of the variant index."""

    MOCK_DATA = [
        ("v1", "c1", 2, "T", "A", ["rs1"]),
        ("v2", "c1", 3, "T", "A", ["rs2", "rs3"]),
        ("v3", "c1", 4, "T", "A", None),
        ("v4", "c1", 5, "T", "A", None),
    ]

    MOCK_SCHEMA = t.StructType(
        [
            t.StructField("variantId", t.StringType(), False),
            t.StructField("chromosome", t.StringType(), False),
            t.StructField("position", t.IntegerType(), False),
            t.StructField("referenceAllele", t.StringType(), False),
            t.StructField("alternateAllele", t.StringType(), False),
            t.StructField("rsIds", t.ArrayType(t.StringType(), True), True),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(self: TestVariantIndex, spark: SparkSession) -> None:
        # Create dataframe:
        self.df = spark.createDataFrame(self.MOCK_DATA, schema=self.MOCK_SCHEMA)
        self.variant_index = VariantIndex(
            _df=self.df, _schema=VariantIndex.get_schema()
        )

    def test_init_type(self: TestVariantIndex) -> None:
        """Just make sure the right datatype is created."""
        assert isinstance(self.variant_index, VariantIndex)

    def test_removed_null_values(self: TestVariantIndex) -> None:
        """Making sure upon initialisation, the null values are removed and were replaced with empty arrays."""
        # RsIds column cannot be null:
        assert self.variant_index.df.filter(f.col("rsIds").isNull()).count() == 0

        # However can be empty array:
        assert self.variant_index.df.filter(f.size("rsIds") == 0).count() > 0
