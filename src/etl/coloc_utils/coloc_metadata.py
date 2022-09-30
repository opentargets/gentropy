"""Functions to add metadata to colocation results."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def add_moleculartrait_phenotype_genes(
    spark: SparkSession, coloc_result: DataFrame, phenotype2gene_path: str
) -> DataFrame:
    """Add Ensembl gene id to molecular trait phenotype IDs.

    Args:
        spark (SparkSession): Spark session
        coloc_result: Results from colocalisation analysis
        phenotype2gene_path: Path of lookup table

    Returns:
        DataFrame: Coloc datasets with gene IDs for molecular trait phenotypes
    """
    # Mapping between molecular trait phenotypes and genes
    phenotype_id = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .csv(phenotype2gene_path)
        .select(
            f.col("phenotype_id").alias("right_phenotype"),
            f.col("gene_id").alias("right_gene_id"),
        )
    )

    coloc_with_metadata = (
        coloc_result.join(
            f.broadcast(phenotype_id),
            on="right_phenotype",
            how="left",
        )
        .withColumn(
            "right_gene_id",
            f.when(
                f.col("right_phenotype").startswith("ENSG"),
                f.col("right_phenotype"),
            ).otherwise(f.col("right_gene_id")),
        )
        .withColumn(
            "right_gene_id",
            f.when(
                f.col("right_study") == "GTEx-sQTL",
                f.regexp_extract(f.col("right_phenotype"), ":(ENSG.*)$", 1),
            ).otherwise(f.col("right_gene_id")),
        )
    )
    return coloc_with_metadata


def add_coloc_sumstats_info(
    spark: SparkSession, coloc: DataFrame, sumstats_path: str
) -> DataFrame:
    """Adds relevant metadata to colocalisation results from summary stats.

    Args:
        spark (SparkSession): Spark session
        coloc (DataFrame): Colocalisation results
        sumstats_path (str): Summary stats dataset

    Returns:
        DataFrame: Colocalisation results with summary stats metadata added
    """
    sumstats_leftvar_rightstudy = (
        # sumstats_path ~250Gb dataset
        spark.read.parquet(sumstats_path)
        .repartition("chrom")
        .withColumn(
            "right_studyKey",
            f.xxhash64(*["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        .withColumn(
            "left_lead_variant_id",
            f.concat_ws("_", f.col("chrom"), f.col("pos"), f.col("ref"), f.col("alt")),
        )
        .withColumnRenamed("chrom", "left_chrom")
        .withColumnRenamed("beta", "left_var_right_study_beta")
        .withColumnRenamed("se", "left_var_right_study_se")
        .withColumnRenamed("pval", "left_var_right_study_pval")
        .withColumnRenamed("is_cc", "left_var_right_isCC")
        # Only keep required columns
        .select(
            "left_chrom",
            "left_lead_variant_id",
            "right_studyKey",
            "left_var_right_study_beta",
            "left_var_right_study_se",
            "left_var_right_study_pval",
            "left_var_right_isCC",
        )
    )

    # join info from sumstats
    coloc_with_metadata = (
        sumstats_leftvar_rightstudy.join(
            f.broadcast(coloc),
            on=["left_chrom", "left_lead_variant_id", "right_studyKey"],
            how="right",
        )
        # clean unnecessary columns
        .drop("left_lead_variant_id", "right_lead_variant_id")
        .drop("left_bio_feature", "left_phenotype")
        .drop("left_studyKey", "right_studyKey")
    )

    return coloc_with_metadata
