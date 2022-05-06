from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from omegaconf import DictConfig


def addMolecularTraitPhenotypeGenes(
    spark: SparkSession, colocResult: SparkSession, phenotype2genePath
):

    # Mapping between molecular trait phenotypes and genes
    phenotypeIdGene = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .csv(phenotype2genePath)
        .select(
            F.col("phenotype_id").alias("right_phenotype"),
            F.col("gene_id").alias("right_gene_id"),
        )
    )

    colocWithMetadata = (
        colocResult.join(
            F.broadcast(phenotypeIdGene),
            on="right_phenotype",
            how="left",
        )
        .withColumn(
            "right_gene_id",
            F.when(
                F.col("right_phenotype").startswith("ENSG"), F.col("right_phenotype")
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
    return colocWithMetadata


def addColocSumstatsInfo(spark: SparkSession, coloc, sumstatsPath: str):

    sumstatsLeftVarRightStudyInfo = (
        # sumstatsPath ~250Gb dataset
        spark.read.parquet(sumstatsPath)
        .repartition("chrom")
        .withColumn(
            "right_studyKey",
            F.concat_ws("_", *["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        .withColumn(
            "left_lead_variant_id",
            F.concat_ws("_", F.col("chrom"), F.col("pos"), F.col("ref"), F.col("alt")),
        )
        .withColumnRenamed("beta", "left_var_right_study_beta")
        .withColumnRenamed("se", "left_var_right_study_se")
        .withColumnRenamed("pval", "left_var_right_study_pval")
        .withColumnRenamed("is_cc", "left_var_right_isCC")
        # Only keep required columns
        .select(
            "left_lead_variant_id",
            "right_studyKey",
            "left_var_right_study_beta",
            "left_var_right_study_se",
            "left_var_right_study_pval",
            "left_var_right_isCC",
        )
    )

    # join info from sumstats
    colocWithMetadata = sumstatsLeftVarRightStudyInfo.join(
        coloc,
        on=["left_lead_variant_id", "right_studyKey"],
        how="right",
    )

    # clean unnecessary columns
    result = (
        colocWithMetadata.drop("left_lead_variant_id", "right_lead_variant_id")
        .drop("left_bio_feature", "left_phenotype")
        .drop("left_studyKey", "right_studyKey")
    )

    return result
