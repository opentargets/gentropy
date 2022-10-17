"""Helper functions to generate the variant index."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession

from etl.common.spark_helpers import get_record_with_minimum_value, nullify_empty_array


def main(
    etl: ETLSession,
    variant_annotation_path: str,
    credible_sets_path: str,
    variants_partition_count: int,
) -> tuple[DataFrame, DataFrame]:
    """Main function to generate a variant index from the variant annotation dataset."""
    etl.logger.info("Generating variant index...")

    # Join the variant index with the credible sets
    variant_idx, invalid_variants = join_variants_w_credset(
        etl, variant_annotation_path, credible_sets_path, variants_partition_count
    )

    return variant_idx, invalid_variants


def join_variants_w_credset(
    etl: ETLSession,
    variant_annotation_path: str,
    credible_sets_path: str,
    partition_count: int,
) -> tuple[DataFrame, DataFrame]:
    """Returns two dataframes: one is the variant index, filtered to contain only the variants present in the credible sets; the second is the variants of the credible set that are not present in the variant annotation.

    Args:
        etl (ETLSession): ETLSession
        variant_annotation_path (str): The path to the variant annotation file
        credible_sets_path (str): the path to the credible sets file
        partition_count (int): The number of partitions to use for the output

    Returns:
        variant_idx (DataFrame): A dataframe with all the variants of interest and their annotation
        fallen_variants (DataFrame): A dataframe with the variants of the credible filtered out of the variant index
    """
    _df = (
        read_variant_annotation(etl, variant_annotation_path)
        .join(
            f.broadcast(get_variants_from_credset(etl, credible_sets_path)),
            on=["id", "chromosome"],
            how="right",
        )
        .withColumn(
            "variantInGnomad", f.coalesce(f.col("variantInGnomad"), f.lit(False))
        )
    )

    variant_idx = (
        _df.filter(f.col("variantInGnomad"))
        .drop("variantInGnomad")
        .coalesce(partition_count)
    )
    invalid_variants = _df.filter(~f.col("variantInGnomad"))

    return variant_idx, invalid_variants


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
    return etl.read_parquet(variant_annotation_path, "variant.json").select(
        *unchanged_cols,
        # schema of the variant index is the same as the variant annotation
        # except for `vep` which is slimmed
        f.struct(
            f.col("vep.mostSevereConsequence").alias("mostSevereConsequence"),
            f.col("vep.regulatoryFeatureConsequences").alias(
                "regulatoryFeatureConsequences"
            ),
            f.col("vep.motifFeatureConsequences").alias("motifFeatureConsequences"),
            f.struct(
                f.col("vep.transcriptConsequences.gene_id").alias("geneId"),
                f.col("vep.transcriptConsequences.transcript_id").alias("transcriptId"),
                f.col("vep.transcriptConsequences.consequence_terms").alias(
                    "consequenceTerms"
                ),
                "vep.distance",
                "vep.transcriptConsequences.lof",
                f.col("vep.transcriptConsequences.lof_flags").alias("lofFlags"),
                f.col("vep.transcriptConsequences.lof_filter").alias("lofFilter"),
                f.col("vep.transcriptConsequences.lof_info").alias("lofInfo"),
                f.col("vep.transcriptConsequences.polyphen_score").alias(
                    "polyphenScore"
                ),
                f.col("vep.transcriptConsequences.sift_score").alias("siftScore"),
            ).alias("transcriptConsequences"),
        ).alias("vep"),
        # filters/rsid are arrays that can be empty, in this case we convert them to null
        nullify_empty_array(f.col("filters")).alias("filters"),
        nullify_empty_array(f.col("rsIds")).alias("rsIds"),
        f.lit(True).alias("variantInGnomad"),
    )


def find_closest_gene(
    variants_df: DataFrame,
    genes_df: DataFrame,
    tss_distance_threshold: int,
) -> DataFrame:
    """For a set of variants, find the closest gene within a certain distance of the variant's position.

    Args:
        variants_df (DataFrame): variants dataset with the columns `id`, `chromosome`, `position`
        genes_df (DataFrame): filtered genes dataset
        tss_distance_threshold (int): The maximum distance from the TSS to consider a variant to be near to a gene

    Returns:
        DataFrame: A dataframe with the variantId, geneId, and distance
    """
    return (
        variants_df.select(f.col("id").alias("variantId"), "chromosome", "position")
        .join(f.broadcast(genes_df), on="chromosome", how="inner")
        .withColumn("distance", f.abs(f.col("position") - f.col("tss")))
        .filter(f.col("distance") <= tss_distance_threshold)
        .transform(
            lambda df: get_record_with_minimum_value(df, "variantId", "distance")
        )
        .select("variantId", "geneId", "distance")
    )
