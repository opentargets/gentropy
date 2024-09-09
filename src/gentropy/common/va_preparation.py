"""Spark operations to prepare VA dataset for efficient per-chromosome joins."""

from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def prepare_va(session: SparkSession, variant_annotation_path: str, tmp_variant_annotation_path: str) -> None:
    """Prepare the VA dataset for efficient per-chromosome joins.

    Args:
        session (SparkSession): The Spark session to be used for reading and writing data.
        variant_annotation_path (str): The path to the input variant annotation dataset.
        tmp_variant_annotation_path (str): The path to store the temporary output for the repartitioned annotation dataset.
    """
    va_df = (
        session
        .spark
        .read
        .parquet(variant_annotation_path)
    )
    va_df_direct = (
        va_df.
        select(
            f.col("chromosome").alias("vaChromosome"),
            f.col("variantId"),
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col("position"),
                f.col("referenceAllele"),
                f.col("alternateAllele")
            ).alias("ukb_ppp_id"),
            f.lit("direct").alias("direction")
        )
    )
    va_df_flip = (
        va_df.
        select(
            f.col("chromosome").alias("vaChromosome"),
            f.col("variantId"),
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col("position"),
                f.col("alternateAllele"),
                f.col("referenceAllele")
            ).alias("ukb_ppp_id"),
            f.lit("flip").alias("direction")
        )
    )
    (
        va_df_direct.union(va_df_flip)
        .coalesce(1)
        .repartition("vaChromosome")
        .write
        .partitionBy("vaChromosome")
        .mode("overwrite")
        .parquet(tmp_variant_annotation_path)
    )
