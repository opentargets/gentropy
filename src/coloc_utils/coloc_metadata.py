"""Functions to add metadata to colocation results
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def add_moleculartrait_phenotype_genes(
    spark: SparkSession, coloc_result, phenotype2gene_path
):
    """Add Ensembl gene id to molecular trait phenotype IDs

    Args:
        spark (SparkSession): Spark session
        coloc_result: Results from colocalisation analysis
        phenotype2gene_path: Path of lookup table

    Returns:
        Dataframe: Coloc datasets with gene IDs for molecular trait phenotypes
    """

    # Mapping between molecular trait phenotypes and genes
    phenotype_id = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .csv(phenotype2gene_path)
        .select(
            F.col("phenotype_id").alias("right_phenotype"),
            F.col("gene_id").alias("right_gene_id"),
        )
    )

    coloc_with_metadata = (
        coloc_result.join(
            F.broadcast(phenotype_id),
            on="right_phenotype",
            how="left",
        )
        .withColumn(
            "right_gene_id",
            F.when(
                F.col("right_phenotype").startswith("ENSG"),
                F.col("right_phenotype"),
            ).otherwise(F.col("right_gene_id")),
        )
        .withColumn(
            "right_gene_id",
            F.when(
                F.col("right_study") == "GTEx-sQTL",
                F.regexp_extract(F.col("right_phenotype"), ":(ENSG.*)$", 1),
            ).otherwise(F.col("right_gene_id")),
        )
    )
    return coloc_with_metadata


def add_coloc_sumstats_info(spark: SparkSession, coloc, sumstats_path: str):
    """Adds relevant metadata to colocalisation results from summary stats

    Args:
        spark (SparkSession): Spark session
        coloc (Dataframe): Colocalisation results
        sumstats_path (str): Summary stats dataset

    Returns:
        _type_: Colocalisation results with summary stats metadata added
    """

    sumstats_leftvar_rightstudy = (
        # sumstats_path ~250Gb dataset
        spark.read.parquet(sumstats_path)
        .repartition("chrom")
        .withColumn(
            "right_studyKey",
            F.xxhash64(*["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        .withColumn(
            "left_lead_variant_id",
            F.concat_ws("_", F.col("chrom"), F.col("pos"), F.col("ref"), F.col("alt")),
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
            F.broadcast(coloc),
            on=["left_chrom", "left_lead_variant_id", "right_studyKey"],
            how="right",
        )
        # clean unnecessary columns
        .drop("left_lead_variant_id", "right_lead_variant_id")
        .drop("left_bio_feature", "left_phenotype")
        .drop("left_studyKey", "right_studyKey")
    )

    return coloc_with_metadata
