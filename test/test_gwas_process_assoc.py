from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession

from src.gwas_ingest.process_associations import filter_assoc_by_rsid

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@pytest.fixture
def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture
def call_rsid_filter(mock_rsid_filter: DataFrame) -> DataFrame:
    return mock_rsid_filter.transform(filter_assoc_by_rsid).persist()


@pytest.fixture
def mock_rsid_filter(get_spark: SparkSession) -> DataFrame:
    return get_spark.createDataFrame(
        [
            # Assoc id 1: matching rsId exist:
            {
                "association_id": 1,
                "rsid_gwas_catalog": ["rs123", "rs523"],
                "rsid_gnomad": ["rs123"],
                "retain": True,
                "drop": False,
            },
            {
                "association_id": 1,
                "rsid_gwas_catalog": ["rs123", "rs523"],
                "rsid_gnomad": ["rs12"],
                "retain": False,
                "drop": True,
            },
            # Assoc id 2: matching rsId exist:
            {
                "association_id": 2,
                "rsid_gwas_catalog": ["rs523"],
                "rsid_gnomad": [],
                "retain": False,
                "drop": True,
            },
            {
                "association_id": 2,
                "rsid_gwas_catalog": ["rs523"],
                "rsid_gnomad": ["rs12", "rs523"],
                "retain": True,
                "drop": False,
            },
            # Assoc id 3: matching rsId doesn't exists, so keep all:
            {
                "association_id": 3,
                "rsid_gwas_catalog": ["rs123"],
                "rsid_gnomad": [],
                "retain": True,
                "drop": False,
            },
            {
                "association_id": 3,
                "rsid_gwas_catalog": ["rs123"],
                "rsid_gnomad": ["rs643", "rs523"],
                "retain": True,
                "drop": False,
            },
            # Assoc id 4: two matching rsids exist, keep both:
            {
                "association_id": 4,
                "rsid_gwas_catalog": ["rs123", "rs523"],
                "rsid_gnomad": ["rs123"],
                "retain": True,
                "drop": False,
            },
            {
                "association_id": 4,
                "rsid_gwas_catalog": ["rs123", "rs523"],
                "rsid_gnomad": ["rs523"],
                "retain": True,
                "drop": False,
            },
            {
                "association_id": 4,
                "rsid_gwas_catalog": ["rs123", "rs523"],
                "rsid_gnomad": ["rs666"],
                "retain": False,
                "drop": True,
            },
        ]
    ).persist()


def test_filter_assoc_by_rsid__all_columns_are_there(
    mock_rsid_filter: DataFrame, call_rsid_filter: DataFrame
) -> None:
    """Testing if the returned dataframe contains all columns from the source:"""
    source_columns = mock_rsid_filter.columns
    processed_columns = call_rsid_filter.columns

    assert any([column in processed_columns for column in source_columns])


def test_filter_assoc_by_rsid__right_rows_are_dropped(
    call_rsid_filter: DataFrame,
) -> None:
    """Testing if all the retained columns should not be dropped."""

    dropped = call_rsid_filter.transform(filter_assoc_by_rsid).select("drop").collect()
    assert not any([d["drop"] for d in dropped])


def test_filter_assoc_by_rsid__right_rows_are_kept(
    call_rsid_filter: DataFrame,
) -> None:
    """Testing if all the retained columns should be kept."""

    kept = call_rsid_filter.transform(filter_assoc_by_rsid).select("retain").collect()
    assert all([d["retain"] for d in kept])
