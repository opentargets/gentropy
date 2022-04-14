"""
Compute all vs all Bayesian colocalisation analysis for all Genetics Portal

This script calculates posterior probabilities of different causal variants
configurations under the assumption of a single causal variant for each trait.

Logic reproduced from: https://github.com/chr1swallace/coloc/blob/main/R/claudia.R
"""

import os
from functools import reduce
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.ml.functions as Fml
from pyspark.ml.linalg import VectorUDT
import hydra
from omegaconf import DictConfig
from utils import getLogsum, getPosteriors


@hydra.main(config_path=os.getcwd(), config_name="config")
def main(cfg: DictConfig) -> None:
    """
    Run colocalisation analysis
    It runs in 3 blocks:
        1. Calculate overlapping signals in OT genetics portal
        2. Perform colocalisation
        3. Add metadata to results
    """

    sparkConf = SparkConf()
    sparkConf = sparkConf.set("spark.hadoop.fs.gs.requester.pays.mode", "AUTO")
    sparkConf = sparkConf.set(
        "spark.hadoop.fs.gs.requester.pays.project.id", "open-targets-eu-dev"
    )

    # establish spark connection
    spark = SparkSession.builder.config(conf=sparkConf).master("local[*]").getOrCreate()

    # register udfs
    logsum = F.udf(getLogsum, T.DoubleType())
    posteriors = F.udf(getPosteriors, VectorUDT())

    credSet = (
        spark.read.parquet(cfg.coloc.credible_set)
        .distinct()
        .withColumn(
            "studyKey",
            F.concat_ws("_", *["type", "study_id", "phenotype_id", "bio_feature"]),
        )
    )

    # Priors
    # priorc1 Prior on variant being causal for trait 1
    # priorc2 Prior on variant being causal for trait 2
    # priorc12 Prior on variant being causal for traits 1 and 2
    priors = spark.createDataFrame(
        [(cfg.coloc.priorc1, cfg.coloc.priorc2, cfg.coloc.priorc12)],
        ("priorc1", "priorc2", "priorc12"),
    )

    columnsToJoin = ["studyKey", "tag_variant_id", "lead_variant_id", "type", "logABF"]
    renameColumns = ["studyKey", "lead_variant_id", "type", "logABF"]

    # STEP 1: Find overlapping signals (exploded at the tag variant level)
    leftDf = reduce(
        lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
        renameColumns,
        credSet.select(columnsToJoin).distinct(),
    )
    rightDf = reduce(
        lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
        renameColumns,
        credSet.select(columnsToJoin).distinct(),
    )

    overlappingPeaks = (
        leftDf
        # molecular traits always on the right-side
        .filter(F.col("left_type") == "gwas")
        # Get all study/peak pairs where at least one tagging variant overlap:
        .join(rightDf, on="tag_variant_id", how="inner")
        .filter(
            # Remove rows with identical study:
            (F.col("left_studyKey") != F.col("right_studyKey"))
        )
        # Keep only the upper triangle where both study is gwas
        .filter(
            (F.col("right_type") != "gwas")
            | (F.col("left_studyKey") > F.col("right_studyKey"))
        )
        # remove overlapping tag variant isnfo
        .drop("left_logABF", "right_logABF", "tag_variant_id")
        # distinct to get study-pair info
        .distinct()
        .persist()
    )

    overlappingLeft = overlappingPeaks.join(
        leftDf.select(
            "left_studyKey", "left_lead_variant_id", "tag_variant_id", "left_logABF"
        ),
        on=["left_studyKey", "left_lead_variant_id"],
        how="inner",
    )
    overlappingRight = overlappingPeaks.join(
        rightDf.select(
            "right_studyKey", "right_lead_variant_id", "tag_variant_id", "right_logABF"
        ),
        on=["right_studyKey", "right_lead_variant_id"],
        how="inner",
    )

    overlappingSignals = overlappingLeft.alias("a").join(
        overlappingRight.alias("b"),
        on=[
            "tag_variant_id",
            "left_lead_variant_id",
            "right_lead_variant_id",
            "left_studyKey",
            "right_studyKey",
            "right_type",
            "left_type",
        ],
        how="outer",
    )

    # STEP 2: Perform colocalisation analysis
    signalPairsCols = ["studyKey", "lead_variant_id", "type"]
    coloc = (
        overlappingSignals
        # Before summarizing logABF columns nulls need to be filled with 0:
        .fillna(0, subset=["left_logABF", "right_logABF"])
        # Grouping data by peak and collect list of the sums:
        .withColumn("sum_logABF", F.col("left_logABF") + F.col("right_logABF"))
        # Group by overlapping peak and generating dense vectors of logABF:
        .groupBy(
            *["left_" + col for col in signalPairsCols]
            + ["right_" + col for col in signalPairsCols]
        )
        .agg(
            F.count("*").alias("coloc_n_vars"),
            Fml.array_to_vector(F.collect_list(F.col("left_logABF"))).alias(
                "left_logABF"
            ),
            Fml.array_to_vector(F.collect_list(F.col("right_logABF"))).alias(
                "right_logABF"
            ),
            Fml.array_to_vector(F.collect_list(F.col("sum_logABF"))).alias(
                "sum_logABF"
            ),
        )
        # Log sums
        .withColumn("logsum1", logsum(F.col("left_logABF")))
        .withColumn("logsum2", logsum(F.col("right_logABF")))
        .withColumn("logsum12", logsum(F.col("sum_logABF")))
        .drop("left_logABF", "right_logABF", "sum_logABF")
        #
        # Add priors
        .crossJoin(priors)
        # h0-h2
        .withColumn("lH0abf", F.lit(0))
        .withColumn("lH1abf", F.log(F.col("priorc1")) + F.col("logsum1"))
        .withColumn("lH2abf", F.log(F.col("priorc2")) + F.col("logsum2"))
        # h3
        .withColumn("sumlogsum", F.col("logsum1") + F.col("logsum2"))
        # exclude null H3/H4s: due to sumlogsum == logsum12
        .filter(F.col("sumlogsum") != F.col("logsum12"))
        .withColumn("max", F.greatest("sumlogsum", "logsum12"))
        .withColumn(
            "logdiff",
            (
                F.col("max")
                + F.log(
                    F.exp(F.col("sumlogsum") - F.col("max"))
                    - F.exp(F.col("logsum12") - F.col("max"))
                )
            ),
        )
        .withColumn(
            "lH3abf",
            F.log(F.col("priorc1")) + F.log(F.col("priorc2")) + F.col("logdiff"),
        )
        .drop("right_logsum", "left_logsum", "sumlogsum", "max", "logdiff")
        # h4
        .withColumn("lH4abf", F.log(F.col("priorc12")) + F.col("logsum12"))
        # cleaning
        .drop("priorc1", "priorc2", "priorc12", "logsum1", "logsum2", "logsum12")
        # posteriors
        .withColumn(
            "allABF",
            Fml.array_to_vector(
                F.array(
                    F.col("lH0abf"),
                    F.col("lH1abf"),
                    F.col("lH2abf"),
                    F.col("lH3abf"),
                    F.col("lH4abf"),
                )
            ),
        )
        .withColumn("posteriors", Fml.vector_to_array(posteriors(F.col("allABF"))))
        .withColumn("coloc_h0", F.col("posteriors").getItem(0))
        .withColumn("coloc_h1", F.col("posteriors").getItem(1))
        .withColumn("coloc_h2", F.col("posteriors").getItem(2))
        .withColumn("coloc_h3", F.col("posteriors").getItem(3))
        .withColumn("coloc_h4", F.col("posteriors").getItem(4))
        .withColumn("coloc_h4_h3", F.col("coloc_h4") / F.col("coloc_h3"))
        .withColumn("coloc_log2_h4_h3", F.log2(F.col("coloc_h4_h3")))
        # clean up
        .drop("posteriors", "allABF", "lH0abf", "lH1abf", "lH2abf", "lH3abf", "lH4abf")
    )

    (coloc.write.mode("overwrite").parquet(cfg.coloc.output))

    # # STEP 3: Add metadata to results
    # phenotypeIdGene = (
    #     spark.read.option("header", "true")
    #     .option("sep", "\t")
    #     .csv(cfg.coloc.phenotype_id_gene)
    # )

    # # Adding study, variant and study-variant metadata from credible set
    # credSetStudyMeta = credSet.select(
    #     "studyKey",
    #     F.col("study_id").alias("study"),
    #     "bio_feature",
    #     F.col("phenotype_id").alias("phenotype"),
    # ).distinct()

    # credSetVariantMeta = credSet.select(
    #     F.col("lead_variant_id"),
    #     F.col("lead_chrom").alias("chrom"),
    #     F.col("lead_pos").alias("pos"),
    #     F.col("lead_ref").alias("ref"),
    #     F.col("lead_alt").alias("alt"),
    # ).distinct()

    # sumstatsLeftVarRightStudyInfo = (
    #     spark.read.parquet(cfg.coloc.sumstats_filtered)
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

    # colocWithMetadata = (
    #     coloc.join(
    #         reduce(
    #             lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
    #             credSetStudyMeta.columns,
    #             credSetStudyMeta,
    #         ),
    #         on="left_studyKey",
    #         how="left",
    #     )
    #     .join(
    #         reduce(
    #             lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
    #             credSetStudyMeta.columns,
    #             credSetStudyMeta,
    #         ),
    #         on="right_studyKey",
    #         how="left",
    #     )
    #     .join(
    #         reduce(
    #             lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
    #             credSetVariantMeta.columns,
    #             credSetVariantMeta,
    #         ),
    #         on="left_lead_variant_id",
    #         how="left",
    #     )
    #     .join(
    #         reduce(
    #             lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
    #             credSetVariantMeta.columns,
    #             credSetVariantMeta,
    #         ),
    #         on="right_lead_variant_id",
    #         how="left",
    #     )
    #     .join(
    #         sumstatsLeftVarRightStudyInfo,
    #         on=["left_lead_variant_id", "right_studyKey"],
    #         how="left",
    #     )
    #     .drop("left_lead_variant_id", "right_lead_variant_id")
    #     .drop("left_bio_feature", "left_phenotype")
    #     .join(
    #         phenotypeIdGene.select(
    #             F.col("phenotype_id").alias("right_phenotype"),
    #             F.col("gene_id").alias("right_gene_id"),
    #         ),
    #         on="right_phenotype",
    #         how="left",
    #     )
    #     .withColumn(
    #         "right_gene_id",
    #         F.when(
    #             F.col("right_phenotype").startswith("ENSG"), F.col("right_phenotype")
    #         ).otherwise(F.col("right_gene_id")),
    #     )
    #     .withColumn(
    #         "right_gene_id",
    #         F.when(
    #             F.col("right_study") == "GTEx-sQTL",
    #             F.regexp_extract(F.col("right_phenotype"), ":(ENSG.*)$", 1),
    #         ).otherwise(F.col("right_gene_id")),
    #     )
    #     .drop("left_studyKey", "right_studyKey")
    # )

    # # Write output
    # (
    #     colocWithMetadata.write.partitionBy("left_chrom")
    #     .mode("overwrite")
    #     .parquet(cfg.coloc.output)
    # )

    # TODO: compute model averaged effect size ratios
    # https://github.com/tobyjohnson/gtx/blob/9afa9597a51d0ff44536bc5c8eddd901ab3e867c/R/coloc.R#L91

    # For debugging
    # (
    #     coloc
    #     .filter(
    #         (F.col("left_studyKey") == "gwas_NEALE2_20003_1140909872") &
    #         (F.col("right_studyKey") ==
    # "sqtl_GTEx-sQTL_chr22:17791301:17806239:clu_21824:ENSG00000243156_Ovary") &
    #         (F.col("left_lead_variant_id") == "22:16590692:CAA:C") &
    #         (F.col("right_lead_variant_id") == "22:17806438:G:A"))
    #     .show(vertical = True)
    # )


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
