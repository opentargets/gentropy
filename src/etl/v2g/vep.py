"""Helper functions that extracts information about pathogenicity prediction from VEP."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

if TYPE_CHECKING:
    from etl.common.ETLSession import ETLSession


def main(
    etl: ETLSession,
    variant_index_path: str,
    variant_annotation_path: str,
    gene_distance_threshold: int,
    variant_consequence_lut_path: str,
) -> DataFrame:
    """Placeholder."""
    # function that takes the index, gets the vep data and parses it
    va = etl.spark.read.parquet(variant_annotation_path).select(
        "id", "vep.transcriptConsequences"
    )
    vi = etl.spark.read.parquet(variant_index_path).select("id")
    annotated_variants = va.join(vi, on="id", how="inner")

    return parse_vep(
        etl, annotated_variants, gene_distance_threshold, variant_consequence_lut_path
    )


def parse_vep(
    etl: ETLSession,
    variants_df: DataFrame,
    gene_distance_threshold: int,
    variant_consequence_lut_path: str,
) -> DataFrame:
    """Placeholder."""
    # TODO: refactor to accept columns?
    consequences_lut = etl.spark.read.csv(
        variant_consequence_lut_path, sep="\t", header=True
    ).select(
        f.col("Term").alias("variantFunctionalConsequence"),
        f.col("v2g_score").alias("score"),
    )

    vep = (
        variants_df.withColumn("tc", f.explode("transcriptConsequences"))
        .select(
            "id",
            f.col("tc.gene_id").alias("geneId"),
            "tc.consequence_terms",
            f.col("tc.distance").alias("distance"),
        )
        .filter(f.col("distance") <= gene_distance_threshold)
        # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
        .join(
            f.broadcast(consequences_lut), on="variantFunctionalConsequence", how="left"
        )
        # This aggregation is very expensive - can i just keep the first consequence?
        .transform(
            lambda df: get_record_with_maximum_value(df, ["id", "geneId"], "score")
        )
    )
    return vep


def get_record_with_maximum_value(
    df: DataFrame, grouping_col: str | list[str], sorting_col: str | list[str]
) -> DataFrame:
    """Returns the record with the maximum value of the sorting column within each group of the grouping column."""
    w = Window.partitionBy(grouping_col).orderBy(f.col(sorting_col).desc())
    return (
        df.withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number")
    )
