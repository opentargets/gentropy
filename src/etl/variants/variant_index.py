"""Helper functions to generate the variant index."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession

from etl.common.spark_helpers import nullify_empty_array


def join_variants_w_credset(
    etl: ETLSession,
    variant_annotation_path: str,
    credible_sets_path: str,
) -> DataFrame:
    """Returns a dataframe with the variants from the credible sets and their annotation, if the variant is in gnomad.

    Args:
        etl (ETLSession): ETLSession
        variant_annotation_path (str): The path to the variant annotation file
        credible_sets_path (str): the path to the credible sets file

    Returns:
        variant_idx (DataFrame): A dataframe with all the variants of interest and their annotation

    The join is performed in a 2 stage process to minimise data shuffling:
        - `credset` is broadcasted to all executors to join it with the variant annotation.
        - left join with `credset` to bring all the variants of the credible sets.
    """
    va = read_variant_annotation(etl, variant_annotation_path)
    credset = get_variants_from_credset(etl, credible_sets_path)

    credset_gnomad_overlap = va.join(
        f.broadcast(credset), on=["id", "chromosome"], how="inner"
    )
    return credset.join(
        credset_gnomad_overlap, on=["id", "chromosome"], how="left"
    ).withColumn("variantInGnomad", f.coalesce(f.col("variantInGnomad"), f.lit(False)))


def get_variants_from_credset(etl: ETLSession, credible_sets_path: str) -> DataFrame:
    """It reads the credible sets from the given path, extracts the lead and tag variants.

    Args:
        etl (ETLSession): ETLSession
        credible_sets_path (str): the path to the credible sets

    Returns:
        DataFrame: A dataframe with all variants contained in the credible sets
    """
    credset = etl.spark.read.parquet(credible_sets_path).select(
        "leadVariantId",
        "tagVariantId",
        f.split(f.col("leadVariantId"), "_")[0].alias("chromosome"),
    )
    return (
        credset.selectExpr("leadVariantId as id", "chromosome")
        .union(credset.selectExpr("tagVariantId as id", "chromosome"))
        .dropDuplicates(["id"])
    )


def read_variant_annotation(etl: ETLSession, variant_annotation_path: str) -> DataFrame:
    """It reads the variant annotation parquet file and formats it to follow the OTG variant model.

    Args:
        etl (ETLSession): ETLSession
        variant_annotation_path (str): path to the variant annotation parquet file

    Returns:
        DataFrame: A dataframe of variants and their annotation
    """
    unchanged_cols = [
        "id",
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
        "chromosomeB37",
        "positionB37",
        "alleleType",
        "alleleFrequencies",
        "cadd",
    ]

    return etl.read_parquet(variant_annotation_path, "variant_annotation.json").select(
        *unchanged_cols,
        f.col("vep.mostSevereConsequence").alias("mostSevereConsequence"),
        # filters/rsid are arrays that can be empty, in this case we convert them to null
        nullify_empty_array(f.col("filters")).alias("filters"),
        nullify_empty_array(f.col("rsIds")).alias("rsIds"),
        f.lit(True).alias("variantInGnomad"),
    )
