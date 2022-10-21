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
    variant_consequence_lut_path: str,
) -> DataFrame:
    """Extracts variant to gene assignments from the variant index and the features predicted by VEP.

    Args:
        etl (ETLSession): ETL session,
        variant_index_path (str): The path to the OTG variant index
        variant_annotation_path (str): The path to the variant annotation file
        variant_consequence_lut_path (str): The path to the LUT between the functional consequences and their assigned V2G score

    Returns:
        DataFrame: High and medium severity variant to gene assignments
    """
    # function that takes the index, gets the vep data and parses it
    va = (
        etl.spark.read.parquet(variant_annotation_path)
        .filter(f.size("vep.transcriptConsequences") != 0)
        .select("id", "vep.transcriptConsequences")
    )
    vi = etl.spark.read.parquet(variant_index_path).select("id")
    annotated_variants = va.join(vi, on="id", how="inner")

    return parse_vep(etl, annotated_variants, variant_consequence_lut_path)


def parse_vep(
    etl: ETLSession,
    variants_df: DataFrame,
    variant_consequence_lut_path: str,
) -> DataFrame:
    """Creates a dataset with variant to gene assignments based on VEP.

    Args:
        etl (ETLSession): ETL session
        variants_df (DataFrame): Dataframe with two columns: "id" and "transcriptConsequences"
        variant_consequence_lut_path (str): Path to the table with the variant consequences sorted by severity

    Returns:
        DataFrame: High and medium severity variant to gene assignments
    """
    # TODO: test this function
    consequences_lut = etl.spark.read.csv(
        variant_consequence_lut_path, sep="\t", header=True
    ).select(
        f.col("Term").alias("variantFunctionalConsequence"),
        f.col("v2g_score").alias("score"),
    )

    return (
        variants_df.withColumn("tc", f.explode("transcriptConsequences"))
        .select(
            "id",
            f.col("tc.gene_id").alias("geneId"),
            f.explode("tc.consequence_terms").alias("variantFunctionalConsequence"),
        )
        # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
        .join(
            f.broadcast(consequences_lut),
            on="variantFunctionalConsequence",
            how="inner",
        )
        .filter(f.col("score") != 0)
        .transform(
            lambda df: get_record_with_maximum_value(df, ["id", "geneId"], "score")
        )
    )


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
