"""The distance dataset contains gene distance to credible set variants provided that these are found within a certain region."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession

from etl.common.spark_helpers import normalise_column


def main(
    etl: ETLSession,
    variant_index: DataFrame,
    gene_index: DataFrame,
    tss_distance_threshold: int,
) -> DataFrame:
    """Extracts variant to gene assignments for variants falling within a window of a gene's TSS."""
    etl.logger.info("Generating distance related V2G data...")

    return (
        get_variant_distance_to_gene(gene_index, variant_index, tss_distance_threshold)
        .select(
            "*",
            f.lit("distance").alias("datatypeId"),
            f.lit("canonical_tss").alias("datasourceId"),
        )
        .transform(lambda df: score_distance(df, tss_distance_threshold))
    )


def score_distance(df: DataFrame, max_distance: int) -> DataFrame:
    """Scores the distance between a variant and a gene's TSS.

    The inverse of each distance is first calculated so that the closer the variant is to the TSS, the higher the score.

    Args:
        df (DataFrame): The V2G df based on distance.
        max_distance (int): The maximum distance between a variant and a gene's TSS.

    Returns:
        DataFrame: The V2G df with the normalised score column.
    """
    inverse_expr = (max_distance - f.col("distance")).alias("tmp_score")
    return (
        df.select("*", inverse_expr)
        .transform(lambda df: normalise_column(df, "tmp_score", "new_score"))
        .drop("tmp_score")
    )


def get_variant_distance_to_gene(
    gene_df: DataFrame,
    variant_df: DataFrame,
    distance_window: int,
) -> DataFrame:
    """Calculates the distance between a variant and the TSS of a gene."""
    return (
        variant_df.select("variantId", "chromosome", "position")
        .join(f.broadcast(gene_df), on="chromosome", how="inner")
        .withColumn("distance", f.abs(f.col("position") - f.col("tss")))
        .filter(f.col("distance") <= distance_window)
        .select("variantId", "geneId", "distance", "chromosome")
    )
