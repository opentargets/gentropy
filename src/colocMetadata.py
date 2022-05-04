from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from omegaconf import DictConfig


def addColocMetadata(spark: SparkSession, coloc, config: DictConfig):

    credSet = (
        spark.read.parquet(config.coloc.credible_set)
        .distinct()
        .withColumn(
            "studyKey",
            F.concat_ws("_", *["type", "study_id", "phenotype_id", "bio_feature"]),
        )
    )

    # Mapping between molecular trait phenotypes and genes
    phenotypeIdGene = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .csv(config.coloc.phenotype_id_gene)
    )

    # Adding study, variant and study-variant metadata from credible set
    credSetStudyMeta = credSet.select(
        "studyKey",
        F.col("study_id").alias("study"),
        "bio_feature",
        F.col("phenotype_id").alias("phenotype"),
    ).distinct()

    credSetVariantMeta = credSet.select(
        F.col("lead_variant_id"),
        F.col("lead_chrom").alias("chrom"),
        F.col("lead_pos").alias("pos"),
        F.col("lead_ref").alias("ref"),
        F.col("lead_alt").alias("alt"),
    ).distinct()

    # sumstatsLeftVarRightStudyInfo = (
    #     spark.read.parquet(config.coloc.sumstats_filtered)
    #     .withColumn(
    #         "right_studyKey",
    #         F.concat_ws("_", *["type", "study_id", "phenotype_id", "bio_feature"]),
    #     )
    #     .withColumn(
    #         "left_lead_variant_id",
    #         F.concat_ws("_", F.col("chrom"), F.col("pos"), F.col("ref"), F.col("alt")),
    #     )
    #     .withColumnRenamed("beta", "left_var_right_study_beta")
    #     .withColumnRenamed("se", "left_var_right_study_se")
    #     .withColumnRenamed("pval", "left_var_right_study_pval")
    #     .withColumnRenamed("is_cc", "left_var_right_isCC")
    #     # Only keep required columns
    #     .select(
    #         "left_lead_variant_id",
    #         "right_studyKey",
    #         "left_var_right_study_beta",
    #         "left_var_right_study_se",
    #         "left_var_right_study_pval",
    #         "left_var_right_isCC",
    #     )
    # )

    colocWithMetadata = (
        coloc.join(
            reduce(
                lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
                credSetStudyMeta.columns,
                credSetStudyMeta,
            ),
            on="left_studyKey",
            how="left",
        )
        .join(
            reduce(
                lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
                credSetStudyMeta.columns,
                credSetStudyMeta,
            ),
            on="right_studyKey",
            how="left",
        )
        .join(
            reduce(
                lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
                credSetVariantMeta.columns,
                credSetVariantMeta,
            ),
            on="left_lead_variant_id",
            how="left",
        )
        .join(
            reduce(
                lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
                credSetVariantMeta.columns,
                credSetVariantMeta,
            ),
            on="right_lead_variant_id",
            how="left",
        )
        # .join(
        #     sumstatsLeftVarRightStudyInfo,
        #     on=["left_lead_variant_id", "right_studyKey"],
        #     how="left",
        # )
        # .drop("left_lead_variant_id", "right_lead_variant_id")
        # .drop("left_bio_feature", "left_phenotype")
        .join(
            phenotypeIdGene.select(
                F.col("phenotype_id").alias("right_phenotype"),
                F.col("gene_id").alias("right_gene_id"),
            ),
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
        .drop("left_studyKey", "right_studyKey")
    )

    return colocWithMetadata
