"""Tests on effect harmonisation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def mock_variant_index(spark: SparkSession) -> VariantIndex:
    """Mock gene index."""
    vi_schema = parse_spark_schema("variant_index.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="variant_index",
        )
        .withSchema(vi_schema)
        .withColumnSpec("chromosomeB37", percentNulls=0.1)
        .withColumnSpec("positionB37", percentNulls=0.1)
        .withColumnSpec("mostSevereConsequence", percentNulls=0.1)
        # Nested column handling workaround
        # https://github.com/databrickslabs/dbldatagen/issues/135
        # It's a workaround for nested column handling in dbldatagen.
        .withColumnSpec(
            "alleleFrequencies",
            expr='array(named_struct("alleleFrequency", rand(), "populationName", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "cadd",
            expr='named_struct("phred", cast(rand() AS float), "raw", cast(rand() AS float))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "filters", expr="array(cast(rand() AS string))", percentNulls=0.1
        )
        .withColumnSpec("rsIds", expr="array(cast(rand() AS string))", percentNulls=0.1)
    )
    data_spec.build().printSchema()

    return VariantIndex(
        _df=data_spec.build(), path="mock_variant_index.parquet", _schema=vi_schema
    )


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)


# @pytest.fixture
# dzzzef call_get_reverse_complement(gene_index: GeneIndex) -> DataFrame:
#     """Test reverse complement on mock data."""
#     return mock_allele_columns.transform(
#         lambda df: get_reverse_complement(df, "allele")
#     )


# def test_reverse_complement(call_get_reverse_complement: DataFrame) -> None:
#     """Test reverse complement."""
#     assert (
#         call_get_reverse_complement.filter(
#             f.col("reverseComp") != f.col("revcomp_allele")
#         ).count()
#     ) == 0
