"""Tests on effect harmonisation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def mock_v2g(spark: SparkSession) -> V2G:
    """Mock v2g dataset."""
    v2g_schema = parse_spark_schema("v2g.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="v2g",
        )
        .withSchema(v2g_schema)
        .withColumnSpec("resourceScore", percentNulls=0.1)
        .withColumnSpec("pmid", percentNulls=0.1)
        .withColumnSpec("biofeature", percentNulls=0.1)
        .withColumnSpec("score", percentNulls=0.1)
        .withColumnSpec("label", percentNulls=0.1)
        .withColumnSpec("variantFunctionalConsequenceId", percentNulls=0.1)
        .withColumnSpec("isHighQualityPlof", percentNulls=0.1)
    )

    return V2G(_df=data_spec.build(), path="mock_v2g.parquet", _schema=v2g_schema)


def test_v2g_creation(mock_v2g: V2G) -> None:
    """Test v2g creation with mock data."""
    assert isinstance(mock_v2g, V2G)


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
