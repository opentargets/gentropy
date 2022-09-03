from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from src.gwas_ingest.process_associations import (
    concordance_filter,
    filter_assoc_by_rsid,
)


@pytest.fixture
def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture
def mock_concordance_filter_data(get_spark: SparkSession) -> DataFrame:
    return get_spark.createDataFrame(
        [
            {
                "id": 0,
                "risk_allele": "A",
                "ref": "A",
                "alt": "T",
                "concordant": True,
            },  # Concordant positive, ref.
            {
                "id": 1,
                "risk_allele": "A",
                "ref": "G",
                "alt": "A",
                "concordant": True,
            },  # Concordant positive, alt.
            {
                "id": 2,
                "risk_allele": "A",
                "ref": "T",
                "alt": "G",
                "concordant": True,
            },  # Concordant negative, ref.
            {
                "id": 3,
                "risk_allele": "A",
                "ref": "G",
                "alt": "T",
                "concordant": True,
            },  # Concordant negative, alt.
            {
                "id": 4,
                "risk_allele": "?",
                "ref": "G",
                "alt": "T",
                "concordant": True,
            },  # Concordant ambigious.
            {
                "id": 5,
                "risk_allele": "ATCG",
                "ref": "C",
                "alt": "T",
                "concordant": False,
            },  # discordant.
        ]
    ).persist()


@pytest.fixture
def call_concordance_filter(mock_concordance_filter_data: DataFrame) -> DataFrame:
    return mock_concordance_filter_data.transform(concordance_filter).persist()


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


def test_concordance_filter__type(call_concordance_filter: DataFrame) -> None:
    """Testing if the function returns the right type"""
    assert isinstance(call_concordance_filter, DataFrame)


def test_concordance_filter__all_columns_returned(
    call_concordance_filter: DataFrame, mock_concordance_filter_data: DataFrame
) -> None:
    """Testing if the function returns the right type"""
    source_columns = mock_concordance_filter_data.columns
    processed_columns = call_concordance_filter.columns

    assert any([column in processed_columns for column in source_columns])


def test_concordance_filter__right_rows_retained(
    call_concordance_filter: DataFrame, mock_concordance_filter_data: DataFrame
) -> None:
    """Testing if the filter generated the expected output."""
    target_ids = [
        row["id"]
        for row in (
            mock_concordance_filter_data.filter(f.col("concordant"))
            .select("id")
            .orderBy("id")
            .collect()
        )
    ]
    filtered_ids = [
        row["id"]
        for row in (call_concordance_filter.select("id").orderBy("id").collect())
    ]

    assert filtered_ids == target_ids
