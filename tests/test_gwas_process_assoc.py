"""Tests to assess gwas catalog ingestion."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from etl.gwas_ingest.process_associations import (
    concordance_filter,
    filter_assoc_by_maf,
    filter_assoc_by_rsid,
)


@pytest.fixture
def mock_maf_filter_data(spark: SparkSession) -> DataFrame:
    """Mock minor allele frequency DataFrame for filtering.

    Args:
        spark (SparkSession): Spark session

    Returns:
        DataFrame: Mock minor allele frequency DataFrame
    """
    return (
        spark.createDataFrame(
            [
                # Simple case:
                {"aid": 1, "pop1": 0.1, "pop2": 0.4, "keep": True},
                {"aid": 1, "pop1": 0.1, "pop2": 0.2, "keep": False},
                # Flip AF -> MAF required:
                {"aid": 2, "pop1": 0.9, "pop2": 0.6, "keep": True},
                {"aid": 2, "pop1": 0.1, "pop2": 0.8, "keep": False},
                # Missing values handled properly:
                {"aid": 3, "pop1": None, "pop2": 0.1, "keep": False},
                {"aid": 3, "pop1": 0.1, "pop2": 0.2, "keep": True},
                {"aid": 4, "pop1": None, "pop2": 0.6, "keep": True},
                {"aid": 4, "pop1": 0.1, "pop2": 0.3, "keep": False},
            ]
        )
        .withColumn(
            "alleleFrequencies",
            f.struct(f.col("pop1").alias("pop1"), f.col("pop2").alias("pop2")),
        )
        .select(
            f.col("aid").alias("associationId"),
            "alleleFrequencies",
            "keep",
            f.monotonically_increasing_id().alias("id"),
        )
        .persist()
    )


@pytest.fixture
def call_maf_filter(mock_maf_filter_data: DataFrame) -> DataFrame:
    """Test filter association by MAF based on mock DataFrame."""
    return mock_maf_filter_data.transform(filter_assoc_by_maf)


@pytest.fixture
def mock_concordance_filter_data(spark: SparkSession) -> DataFrame:
    """Mock DataFrame to assess allele concordance.

    Args:
        spark (SparkSession): Spark session

    Returns:
        DataFrame: Mock allele concordances
    """
    data = [
        (
            0,
            "A",
            "A",
            "T",
            True,
        ),  # Concordant positive, ref.
        (
            1,
            "A",
            "G",
            "A",
            True,
        ),  # Concordant positive, alt.
        (
            2,
            "A",
            "T",
            "G",
            True,
        ),  # Concordant negative, ref.
        (
            3,
            "A",
            "G",
            "T",
            True,
        ),  # Concordant negative, alt.
        (
            4,
            "?",
            "G",
            "T",
            True,
        ),  # Concordant ambigious.
        (
            5,
            "ATCG",
            "C",
            "T",
            False,
        ),  # discordant.
    ]
    df = spark.createDataFrame(data, ["id", "riskAllele", "ref", "alt", "concordant"])
    return df


@pytest.fixture
def call_concordance_filter(mock_concordance_filter_data: DataFrame) -> DataFrame:
    """Test allele concordance filter based on mock DataFrame."""
    return mock_concordance_filter_data.transform(concordance_filter)


@pytest.fixture
def call_rsid_filter(mock_rsid_filter: DataFrame) -> DataFrame:
    """Test filter association by rsid based on mock DataFrame."""
    return mock_rsid_filter.transform(filter_assoc_by_rsid)


@pytest.fixture
def mock_rsid_filter(spark: SparkSession) -> DataFrame:
    """Mock DataFrame to evaluate rsids.

    Args:
        spark (SparkSession): Spark session

    Returns:
        DataFrame: Configurations of rsids in resources and expected outcomes
    """
    data = [
        # Assoc id 1: matching rsId exist:
        (
            1,
            ["rs123", "rs523"],
            ["rs123"],
            True,
            False,
        ),
        (
            1,
            ["rs123", "rs523"],
            ["rs12"],
            False,
            True,
        ),
        # Assoc id 2: matching rsId exist:
        (
            2,
            ["rs523"],
            [],
            False,
            True,
        ),
        (
            2,
            ["rs523"],
            ["rs12", "rs523"],
            True,
            False,
        ),
        # Assoc id 3: matching rsId doesn't exists, so keep all:
        (
            3,
            ["rs123"],
            [],
            True,
            False,
        ),
        (
            3,
            ["rs123"],
            ["rs643", "rs523"],
            True,
            False,
        ),
        # Assoc id 4: two matching rsids exist, keep both:
        (
            4,
            ["rs123", "rs523"],
            ["rs123"],
            True,
            False,
        ),
        (
            4,
            ["rs123", "rs523"],
            ["rs523"],
            True,
            False,
        ),
        (
            4,
            ["rs123", "rs523"],
            ["rs666"],
            False,
            True,
        ),
    ]
    df = spark.createDataFrame(
        data, ["associationId", "rsidGwasCatalog", "rsidGnomad", "retain", "drop"]
    )
    return df


def test_filter_assoc_by_rsid__all_columns_are_there(
    mock_rsid_filter: DataFrame, call_rsid_filter: DataFrame
) -> None:
    """Testing if the returned dataframe contains all columns from the source."""
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
    """Testing if the function returns the right type."""
    assert isinstance(call_concordance_filter, DataFrame)


def test_concordance_filter__all_columns_returned(
    call_concordance_filter: DataFrame, mock_concordance_filter_data: DataFrame
) -> None:
    """Testing if the function returns the right type."""
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


def test_maf_filter__right_rows_retained(
    call_maf_filter: DataFrame, mock_maf_filter_data: DataFrame
) -> None:
    """Testing if the filter generated the expected output."""
    target_ids = [
        row["id"]
        for row in (mock_maf_filter_data.filter(f.col("keep")).orderBy("id").collect())
    ]
    filtered_ids = [row["id"] for row in (call_maf_filter.orderBy("id").collect())]

    assert filtered_ids == target_ids
