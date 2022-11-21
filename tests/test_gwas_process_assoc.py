"""Tests to assess gwas catalog ingestion."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from etl.gwas_ingest.process_associations import (
    concordance_filter,
    deconvolute_variants,
    filter_assoc_by_maf,
    filter_assoc_by_rsid,
)


@pytest.fixture
def mock_variant_deconvolution_data(spark: SparkSession) -> DataFrame:
    """Mocking data for variant deconvolution.

    It creates a DataFrame with four rows, each of which has a different combination of values for the
    columns `chromosome`, `position`, `strongestSnpRiskAllele`, and `snpIds`

    Args:
        spark (SparkSession): SparkSession

    Returns:
        A dataframe with the following columns:
            - chromosome
            - position
            - strongestSnpRiskAllele
            - snpIds
            - flag
            - to_be_flagged
    """
    return spark.createDataFrame(
        [
            {
                "chromosome": "12",
                "position": "1063",
                "strongestSnpRiskAllele": "rs233_T",
                "snpIds": "rs233",
                "flag": ["Some other flag"],
                "to_be_flagged": False,
            },
            {
                "chromosome": "3",
                "position": "235423",
                "strongestSnpRiskAllele": "rs23423_A",
                "snpIds": "rs23423",
                "flag": [],
                "to_be_flagged": False,
            },
            {
                "chromosome": "4",
                "position": "944423",
                "strongestSnpRiskAllele": "rs23423_A; rs233_A",
                "snpIds": "rs23423; rs233_A",
                "flag": [],
                "to_be_flagged": True,
            },
            {
                "chromosome": "4;4",
                "position": "944423;9444",
                "strongestSnpRiskAllele": "rs23423_A; rs233_A",
                "snpIds": "rs23423; rs233_A",
                "flag": [],
                "to_be_flagged": False,
            },
        ]
    ).persist()


@pytest.fixture
def call_maf_filter(mock_data_for_maf_filter: DataFrame) -> DataFrame:
    """Test filter association by MAF based on mock DataFrame."""
    return mock_data_for_maf_filter.transform(filter_assoc_by_maf)


@pytest.fixture
def mock_concordance_filter_data(spark: SparkSession) -> DataFrame:
    """Mock DataFrame to assess allele concordance.

    Args:
        spark (SparkSession): Spark session

    Returns:
        DataFrame: Mock allele concordances
    """
    data = [
        (0, "A", "A", "T", True, "1_12_A_T"),  # Concordant positive, ref.
        (1, "A", "G", "A", True, "1_12_G_A"),  # Concordant positive, alt.
        (2, "A", "T", "G", True, "1_12_T_G"),  # Concordant negative, ref.
        (3, "A", "G", "T", True, "1_12_G_T"),  # Concordant negative, alt.
        (4, "?", "G", "T", True, "1_12_G_T"),  # Concordant ambigious.
        (5, "ATCG", "C", "T", False, "1_12_C_T"),  # discordant.
        (6, None, None, None, True, None),  # No mapping, still considered good.
    ]
    return spark.createDataFrame(
        data,
        [
            "id",
            "riskAllele",
            "referenceAllele",
            "alternateAllele",
            "concordant",
            "variantId",
        ],
    )


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
        DataFrame: Combinations of source and mapped rsids and expected outcomes
    """
    data = [
        # Assoc id 1: matching rsId exist:
        (1, ["rs123", "rs523"], ["rs123"], True, False, "1_12_A_C"),
        (1, ["rs123", "rs523"], ["rs12"], False, True, "1_12_A_G"),
        # Assoc id 2: matching rsId exist:
        (2, ["rs523"], [], False, True, "1_12_A_C"),
        (2, ["rs523"], ["rs12", "rs523"], True, False, "1_1122_GG_C"),
        # Assoc id 3: matching rsId doesn't exists, so keep all:
        (3, ["rs123"], [], True, False, "1_20392_A_GG"),
        (3, ["rs123"], ["rs643", "rs523"], True, False, "1_20392_A_C"),
        # Assoc id 4: two matching rsids exist, keep both:
        (4, ["rs123", "rs523"], ["rs123"], True, False, "1_12_A_G"),
        (4, ["rs123", "rs523"], ["rs523"], True, False, "1_12_A_C"),
        (4, ["rs123", "rs523"], ["rs666"], False, True, "1_12_A_T"),
        # Assoc id 5: no mapping, however we keep the row:
        (5, ["rs123", "rs523"], ["rs123"], True, False, None),
    ]
    return spark.createDataFrame(
        data,
        [
            "associationId",
            "rsIdsGwasCatalog",
            "rsIdsGnomad",
            "retain",
            "drop",
            "variantId",
        ],
    )


@pytest.fixture
def mock_data_for_maf_filter(spark: SparkSession) -> DataFrame:
    """Mocking data for MAF filter.

    It creates a dataframe with 2 associations that were mapped to 3 and 0 GnomAD variants

    Args:
        spark (SparkSession): SparkSession

    Returns:
        A dataframe with 2 associations that were mapped to 3 and 0 GnomAD variants.
    """
    return spark.createDataFrame(
        [
            {
                "associationId": 1,
                "variantId": "v_1",
                "alleleFrequencies": [
                    {"alleleFrequency": 0.12, "populationName": "nfe"},
                    {"alleleFrequency": 0.88, "populationName": "afr"},
                ],
                "keepVariant": False,
            },
            {
                "associationId": 1,
                "variantId": "v_2",
                "alleleFrequencies": [
                    {"alleleFrequency": 0.50, "populationName": "nfe"},
                    {"alleleFrequency": 0.11, "populationName": "afr"},
                ],
                "keepVariant": True,
            },
            {
                "associationId": 1,
                "variantId": "v_3",
                "alleleFrequencies": [
                    {"alleleFrequency": 0.02, "populationName": "nfe"},
                    {"alleleFrequency": 0.01, "populationName": "afr"},
                ],
                "keepVariant": False,
            },
            {
                "associationId": 2,
                "variantId": None,
                "alleleFrequencies": None,
                "keepVariant": True,
            },
        ]
    ).persist()


def test_filter_assoc_by_rsid__all_columns_are_there(
    mock_rsid_filter: DataFrame, call_rsid_filter: DataFrame
) -> None:
    """Testing if the returned dataframe contains all columns from the source."""
    assert mock_rsid_filter.schema == call_rsid_filter.schema


def test_filter_assoc_by_rsid__right_rows_are_dropped(
    call_rsid_filter: DataFrame,
) -> None:
    """Testing if all the retained columns should not be dropped."""
    dropped = call_rsid_filter.transform(filter_assoc_by_rsid).select("drop").collect()
    assert not any(d["drop"] for d in dropped)


def test_filter_assoc_by_rsid__right_rows_are_kept(
    call_rsid_filter: DataFrame,
) -> None:
    """Testing if all the retained columns should be kept."""
    kept = call_rsid_filter.transform(filter_assoc_by_rsid).select("retain").collect()
    assert all(d["retain"] for d in kept)


def test_concordance_filter__type(call_concordance_filter: DataFrame) -> None:
    """Testing if the function returns the right type."""
    assert isinstance(call_concordance_filter, DataFrame)


def test_concordance_filter__all_columns_returned(
    call_concordance_filter: DataFrame, mock_concordance_filter_data: DataFrame
) -> None:
    """Testing if the function returns the right type."""
    source_columns = mock_concordance_filter_data.columns
    processed_columns = call_concordance_filter.columns

    assert any(column in processed_columns for column in source_columns)


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


def test_maf_filter__schema(
    call_maf_filter: DataFrame, mock_data_for_maf_filter: DataFrame
) -> None:
    """Testing if the filter doesn't change schema."""
    target = mock_data_for_maf_filter.filter(f.col("keepVariant"))
    assert call_maf_filter.schema == target.schema


def test_maf_filter__content(
    call_maf_filter: DataFrame, mock_data_for_maf_filter: DataFrame
) -> None:
    """Testing if the filter generated the expected output."""
    target = mock_data_for_maf_filter.filter(f.col("keepVariant"))
    assert call_maf_filter.collect() == target.collect()


def test_variant_deconvolution(mock_variant_deconvolution_data: DataFrame) -> None:
    """Testing variant deconvolution.

    This function tests if the expected rows of the mock dataset got flagged with
    `Variant inconsistency` flag.

    Args:
        mock_variant_deconvolution_data (DataFrame): DataFrame
    """
    assert (
        mock_variant_deconvolution_data.transform(deconvolute_variants)
        .withColumn(
            "isFlagged", f.array_contains(f.col("flag"), "Variant inconsistency")
        )
        .filter(f.col("isFlagged") != f.col("to_be_flagged"))
        .count()
    ) == 0
