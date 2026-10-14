"""Tests for operations on datasets with associations."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.operation.association import pvalue_filter


def _summary_statistics(
    spark: SparkSession, pvalues: list[tuple[str, float, int]]
) -> SummaryStatistics:
    """Summary statistics of one study, one row per (variantId, mantissa, exponent).

    Args:
        spark (SparkSession): Spark session.
        pvalues (list[tuple[str, float, int]]): Variant and p-value of each row.

    Returns:
        SummaryStatistics: The rows, other statistics filled with placeholders.
    """
    rows = [
        ("GCST1", variant_id, "1", 100, 0.1, 5000, mantissa, exponent, 0.3, 0.01)
        for variant_id, mantissa, exponent in pvalues
    ]
    return SummaryStatistics(spark.createDataFrame(rows, SummaryStatistics.schema))


def test_pvalue_filter_keeps_variants_at_least_as_significant_as_the_threshold(
    spark: SparkSession,
) -> None:
    """The exponent decides first; the mantissa only when exponents are equal."""
    summary_statistics = _summary_statistics(
        spark,
        [
            ("smaller_exponent", 9.9, -9),
            ("equal", 5.0, -8),
            ("larger_mantissa", 5.1, -8),
            ("larger_exponent", 1.0, -7),
        ],
    )

    filtered = summary_statistics.pipe(pvalue_filter, 5e-8)

    assert sorted(row.variantId for row in filtered.df.collect()) == [
        "equal",
        "smaller_exponent",
    ]


# Tests ported from the legacy package (legacy_tests/gentropy/dataset/test_summary_statistics.py).


@pytest.mark.legacy
def test_summary_statistics__pval_filter__return_type(spark: SparkSession) -> None:
    """The p-value filter returns the dataset type it was given."""
    summary_statistics = _summary_statistics(spark, [("1_100_A_T", 1.0, -3)])
    assert isinstance(pvalue_filter(summary_statistics, 5e-3), SummaryStatistics)
