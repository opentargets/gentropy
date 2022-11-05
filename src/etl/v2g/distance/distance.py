"""Generation of relationship between a variant and a window of a gene."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession

from src.etl.common.spark_helpers import get_record_with_minimum_value, normalise_column
from src.etl.common.utils import get_gene_tss


def main(
    etl: ETLSession,
    variant_index_path: str,
    gene_index_path: str,
    tss_distance_threshold: int,
) -> DataFrame:
    """Extracts variant to gene assignments for variants falling within a window of a gene's TSS."""
    etl.logger.info("Generating distance related V2G data...")
    variant_index = etl.read_parquet(
        variant_index_path, "variant_index.json"
    ).selectExpr("id as variantId")
    gene_idx = etl.read_parquet(gene_index_path, "targets.json").select(
        f.col("id").alias("geneId"),
        get_gene_tss(
            f.col("genomicLocation.strand"),
            f.col("genomicLocation.start"),
            f.col("genomicLocation.end"),
        ).alias("tss"),
        f.col("genomicLocation.chromosome").alias("chromosome"),
    )
    distances = calculate_dist_to_gene(gene_idx, variant_index, tss_distance_threshold)

    return distances.select(
        "*",
        f.lit("distance").alias("datatypeId"),
        f.lit("canonical_tss").alias("datasourceId"),
    ).transform(score_distance)


def score_distance(df: DataFrame) -> DataFrame:
    """Scores the distance between a variant and a gene's TSS.

    The inverse of each distance is first calculated so that the closer the variant is to the TSS, the higher the score.

    Args:
        df (DataFrame): The V2G df based on distance.

    Returns:
        DataFrame: The V2G df with the normalised score column.
    """
    inverse_expr = (
        f.when(f.col("distance") == 0, 1)
        .otherwise(1 / f.col("distance"))
        .alias("tmp_score")
    )
    return (
        df.select("*", inverse_expr)
        .transform(lambda df: normalise_column(df, "tmp_score", "score"))
        .drop("tmp_score")
    )


def calculate_dist_to_gene(
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
        .transform(
            lambda df: get_record_with_minimum_value(df, "variantId", "distance")
        )
        .select("variantId", "geneId", "distance")
    )
