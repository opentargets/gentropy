"""Values that Spark 4's ANSI mode would raise on give nulls, as they did in Spark 3."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.common.processing import extract_position
from gentropy.common.spark import filter_array_struct
from gentropy.common.stats import (
    split_pvalue_column,
    stderr_from_chi2_and_effect_size,
)
from gentropy.method.ld import LDAnnotator


def test_split_pvalue_column_gives_nulls_for_a_value_that_is_not_a_number(
    spark: SparkSession,
) -> None:
    """A p-value such as NA has no mantissa and no exponent."""
    df = spark.createDataFrame([("NA",), ("1E",), ("0.01",)], "pval STRING")
    rows = df.select("pval", *split_pvalue_column(f.col("pval"))).collect()
    assert {r.pval: (r.pValueMantissa, r.pValueExponent) for r in rows} == {
        "NA": (None, None),
        "1E": (1.0, None),
        "0.01": (1.0, -2),
    }


def test_stderr_from_chi2_and_effect_size_is_null_for_a_chi2_of_zero(
    spark: SparkSession,
) -> None:
    """A p-value of 1 gives a chi2 of 0, which leaves the standard error unknown."""
    df = spark.createDataFrame([(0.0, 1.0)], "chi2 DOUBLE, beta DOUBLE")
    row = df.select(
        stderr_from_chi2_and_effect_size(f.col("chi2"), f.col("beta"))
    ).first()
    assert row is not None
    assert row.standardError is None


def test_extract_position_is_null_for_a_variant_without_position(
    spark: SparkSession,
) -> None:
    """A variant identifier without a position gives a null position, also as an int."""
    df = spark.createDataFrame([("OTVAR_abc",), ("1_100_A_T",)], "variantId STRING")
    rows = df.select(
        "variantId", extract_position(f.col("variantId")).cast("int").alias("pos")
    ).collect()
    assert {r.variantId: r.pos for r in rows} == {"OTVAR_abc": None, "1_100_A_T": 100}


def test_filter_array_struct_is_null_for_a_missing_key(spark: SparkSession) -> None:
    """A key that no struct of the array has gives null."""
    df = spark.createDataFrame(
        [([("a", 1)],)], "items ARRAY<STRUCT<key: STRING, value: INT>>"
    )
    row = df.select(
        filter_array_struct("items", "key", "b", "value").alias("value")
    ).first()
    assert row is not None
    assert row.value is None


def test_major_population_is_null_for_an_empty_ld_population_structure(
    spark: SparkSession,
) -> None:
    """A study without LD populations has no major population."""
    df = spark.createDataFrame(
        [([],)],
        "ldPopulationStructure ARRAY<STRUCT<ldPopulation: STRING, relativeSampleSize: DOUBLE>>",
    )
    row = df.select(
        LDAnnotator._get_major_population(f.col("ldPopulationStructure")).alias(
            "majorPopulation"
        )
    ).first()
    assert row is not None
    assert row.majorPopulation is None
