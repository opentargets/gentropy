from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession

from etl.common.spark_helpers import get_record_with_minimum_value, nullify_empty_array


def get_variants_intersect(
    etl: ETLSession,
    variant_annotation_path: str,
    credible_sets_path: str,
    partition_count: int,
) -> DataFrame:
    """
    It reads the variant annotation file, and subsets it to only contain the variants present in the credible sets

    Args:
      etl (ETLSession): ETLSession
      variant_annotation_path (str): The path to the variant annotation file.
      credible_sets_path (str): the path to the credible sets file

    Returns:
      A dataframe with all the variants of interest and their annotation.
    """
    return (
        read_variant_annotation(etl, variant_annotation_path)
        .join(get_variants_from_credset(etl, credible_sets_path), on="id", how="inner")
        .repartition(partition_count)
    )


def get_variants_from_credset(etl: ETLSession, credible_sets_path: str) -> DataFrame:
    """
    It reads the credible sets from the given path, extracts the lead and tag variants

    Args:
      etl (ETLSession): ETLSession
      credible_sets_path (str): the path to the credible sets

    Returns:
      A dataframe with all variants contained in the credible sets
    """
    credset = etl.spark.read.parquet(credible_sets_path).select(
        "leadVariantId", "tagVariantId"
    )
    return (
        credset.selectExpr("leadVariantId as id")
        .union(credset.selectExpr("tagVariantId as id"))
        .dropDuplicates(["id"])
    )


def read_variant_annotation(etl: ETLSession, variant_annotation_path: str) -> DataFrame:
    """
    It reads the variant annotation parquet file and formats it to follow the OTG variant model.

    Args:
      etl (ETLSession): ETLSession
      variant_annotation_path (str): path to the variant annotation parquet file

    Returns:
      A dataframe of variants and their annotation
    """

    # TODO: read input with read_parquet providing schema
    return (
        etl.spark.read.parquet(variant_annotation_path)
        .select(
            "id",
            f.col("chrom").alias("chromosome"),
            f.col("pos").alias("position"),
            f.col("ref").alias("referenceAllele"),
            f.col("alt").alias("alternateAllele"),
            f.col("chrom_b37").alias("chromosomeB37"),
            f.col("pos_b37").alias("positionB37"),
            f.col("allele_type").alias("alleleType"),
            f.col("rsid").alias("rsId"),
            "alleleFrequencies",
            "cadd",
            # schema of the variant index is the same as the variant annotation
            # except for `vep` which is slimmed and reshaped
            # TODO: convert vep annotation from arr to struct of arrays
            f.struct(
                f.col("vep.most_severe_consequence").alias("mostSevereConsequence"),
                f.col("vep.regulatory_feature_consequences").alias(
                    "regulatoryFeatureConsequences"
                ),
                f.col("vep.motif_feature_consequences").alias(
                    "motifFeatureConsequences"
                ),
                "vep.transcript_consequences.lof",
                f.col("vep.transcript_consequences.lof_flags").alias("lofFlags"),
                f.col("vep.transcript_consequences.lof_filter").alias("lofFilter"),
                f.col("vep.transcript_consequences.lof_info").alias("lofInfo"),
                f.col("vep.transcript_consequences.polyphen_score").alias(
                    "polyphenScore"
                ),
                f.col("vep.transcript_consequences.sift_score").alias("siftScore"),
            ).alias("vep"),
            "filters",
            "rsid",
        )
        # filters/rsid are arrays that can be empty, in this case we convert them to null
        .withColumn("filters", nullify_empty_array(f.col("filters")))
        .withColumn("rsIds", nullify_empty_array(f.col("rsid")))
        .drop("rsid")
    )


def calculate_dist_to_gene(
    gene_idx: DataFrame,
    variant_idx: DataFrame,
    tss_distance_threshold: int,
) -> DataFrame:

    return (
        variant_idx.select(f.col("id").alias("variantId"), "chromosome", "position")
        .join(f.broadcast(gene_idx), on="chromosome", how="inner")
        .withColumn("distance", f.abs(f.col("position") - f.col("tss")))
        .filter(f.col("distance") <= tss_distance_threshold)
        .transform(
            lambda df: get_record_with_minimum_value(df, "variantId", "distance")
        )
        .select("variantId", "geneId", "distance")
    )
