"""Tests to assess gwas catalog ingestion."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

from etl.gwas_ingest.association_mapping import clean_mappings


@pytest.fixture
def mock_mapping_dataset(spark: SparkSession) -> DataFrame:
    """Mock association dataset with arbitrary mappings."""
    data = [
        # Association No1: Although both mappings are concordant, only v2 has matching rsid:
        (1, "A", ["rs123"], "v1", [], "A", "G", [[0.9], [0.5]], False),
        (1, "A", ["rs123"], "v2", ["rs123"], "T", "C", [[0.3], [0.4]], True),
        # Association No2: no mapping, we keep the row:
        (2, "A", ["rs123"], None, None, None, None, None, True),
        # Association No3: No matching rsid, keep concordant variant V3:
        (3, "A", ["rs123"], "v3", [], "A", "G", [[0.9], [0.5]], True),
        (3, "A", ["rs123"], "v4", [], "G", "C", [[0.3], [0.4]], False),
        # Association No4: No matching rsid, all concordant, keep variant with highest MAF- V6:
        (4, "A", ["rs123"], "v5", [], "A", "G", [[0.9], [0.3]], False),
        (4, "A", ["rs123"], "v6", [], "G", "T", [[0.6], [0.2]], True),
    ]
    schema = t.StructType(
        [
            t.StructField("associationId", t.IntegerType(), True),
            t.StructField("riskAllele", t.StringType(), True),
            t.StructField("rsIdsGwasCatalog", t.ArrayType(t.StringType()), True),
            t.StructField("variantId", t.StringType(), True),
            t.StructField("rsIdsGnomad", t.ArrayType(t.StringType()), True),
            t.StructField("referenceAllele", t.StringType(), True),
            t.StructField("alternateAllele", t.StringType(), True),
            t.StructField(
                "alleleFrequencies",
                t.ArrayType(
                    t.StructType(
                        [t.StructField("alleleFrequency", t.FloatType(), True)]
                    )
                ),
            ),
            t.StructField("keptMapping", t.BooleanType(), True),
        ]
    )
    return spark.createDataFrame(data, schema=schema).persist()


@pytest.fixture
def cleaned_mock(mock_mapping_dataset: DataFrame) -> DataFrame:
    """Function to return the cleaned mappings."""
    return clean_mappings(mock_mapping_dataset).persist()


def test_clean_mappings__schema(
    cleaned_mock: DataFrame, mock_mapping_dataset: DataFrame
) -> None:
    """Test to make sure the right columns are returned from the function."""
    assert any(
        column in cleaned_mock.columns for column in mock_mapping_dataset.columns
    )


def test_clean_mappings__rows(
    cleaned_mock: DataFrame, mock_mapping_dataset: DataFrame
) -> None:
    """Test to make sure the right GnomAD mappings are kept."""
    columns = cleaned_mock.columns
    expected = (
        mock_mapping_dataset.select(*columns)
        .filter(f.col("keptMapping"))
        .orderBy("variantId")
        .collect()
    )
    observed = cleaned_mock.select(*columns).orderBy("variantId").collect()
    assert expected == observed
