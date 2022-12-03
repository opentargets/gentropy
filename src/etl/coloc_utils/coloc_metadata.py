"""Functions to add metadata to colocation results."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def add_moleculartrait_phenotype_genes(
    coloc_result: DataFrame, phenotype_id_gene: DataFrame
) -> DataFrame:
    """Add Ensembl gene id to molecular trait phenotype IDs.

    Args:
        coloc_result (DataFrame): Results from colocalisation analysis
        phenotype_id_gene (DataFrame): LUT with the ENSEMBL gene id for each molecular trait phenotype ID

    Returns:
        DataFrame: Coloc datasets with gene IDs for molecular trait phenotypes
    """
    return (
        coloc_result.join(
            f.broadcast(phenotype_id_gene),
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


def add_coloc_sumstats_info(coloc: DataFrame, sumstats: DataFrame) -> DataFrame:
    """Adds relevant metadata to colocalisation results from summary stats.

    Args:
        coloc (DataFrame): Colocalisation results
        sumstats (DataFrame): Summary stats dataset

    Returns:
        DataFrame: Colocalisation results with summary stats metadata added
    """
    sumstats_leftvar_rightstudy = (
        # sumstats_path ~250Gb dataset
        sumstats.repartition("chrom")
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

    return (
        # join info from sumstats
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
