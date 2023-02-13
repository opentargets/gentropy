"""Tests on effect harmonisation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from etl.gwas_ingest.effect_harmonization import harmonize_effect

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.session import SparkSession


@pytest.fixture
def mock_assoc_table(spark: SparkSession) -> DataFrame:
    """Mock Dataframe to test effect harmonisation.

    Args:
        spark (SparkSession): SparkSession

    Returns:
        A dataframe with columns required for effect harmonization
    """
    data = [
        # Beta, no need to harmonization:
        (
            "A",
            "C",
            "C",
            5,
            -11,
            0.6,
            "[0.013-0.024] unit increase",
            True,
            True,
            False,
            False,
            False,
        ),
        # Beta, harmonization required:
        (
            "A",
            "C",
            "T",
            5,
            -11,
            0.6,
            "[0.013-0.024] unit increase",
            True,
            False,
            False,
            False,
            False,
        ),
        # Beta, effect dropped due to palindromic alleles:
        (
            "A",
            "T",
            "A",
            5,
            -11,
            0.6,
            "[0.013-0.024] unit increase",
            False,
            False,
            False,
            False,
            True,
        ),
    ]
    mock_df = (
        spark.createDataFrame(
            data,
            [
                "referenceAllele",
                "alternateAllele",
                "riskAllele",
                "pValueMantissa",
                "pValueExponent",
                "effectSize",
                "confidenceInterval",
                "isBeta",
                "isBetaPositive",
                "isOR",
                "isORPositive",
                "isTestPalindrome",
            ],
        )
        .withColumn("qualityControl", f.array())
        .persist()
    )

    return mock_df


@pytest.fixture
def call_effec_harmonized(mock_assoc_table: DataFrame) -> DataFrame:
    """Test filter association by rsid based on mock DataFrame."""
    return mock_assoc_table.transform(harmonize_effect)


def test_harmonize_effect__betas(call_effec_harmonized: DataFrame) -> None:
    """Testing if the betas are found as expected."""
    betas_count = call_effec_harmonized.filter(
        ~f.col("isBeta")
        .cast("int")
        .bitwiseXOR(f.col("beta").isNotNull().cast("int"))
        .cast("boolean")
    ).count()

    assert betas_count == call_effec_harmonized.count()


def test_harmonize_effect__schema(
    call_effec_harmonized: DataFrame, mock_assoc_table: DataFrame
) -> None:
    """Testing if the expected columns are added and removed."""
    columns_to_drop = [
        "effectSize",
        "confidenceInterval",
        "riskAllele",
    ]
    columns_to_add = [
        "odds_ratio_ci_lower",
        "odds_ratio_ci_upper",
        "odds_ratio",
        "beta",
        "beta_ci_upper",
        "beta_ci_lower",
    ]

    # Generating the new set of columns from the input table:
    expected_columns = [
        col for col in mock_assoc_table.columns if col not in columns_to_drop
    ] + columns_to_add
    expected_columns.sort()

    # Observed columns:
    observed_columns = call_effec_harmonized.columns
    observed_columns.sort()

    assert expected_columns == observed_columns


def test_harmonize_effect__palindrome_flag(call_effec_harmonized: DataFrame) -> None:
    """Test to ensure all palindromic alleles are flagged as such."""
    palindrome_flagged = (
        # Keeping
        call_effec_harmonized.filter(
            f.col("isTestPalindrome") | (f.size(f.col("qualityControl")) > 0)
        )
        .filter(~f.col("isTestPalindrome") | (f.size(f.col("qualityControl")) == 0))
        .count()
    )

    assert palindrome_flagged == 0
