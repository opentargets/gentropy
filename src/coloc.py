"""
Utilities to perform colocalisation analysis
"""
import numpy as np
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import pyspark.ml.functions as Fml


def getLogsum(logABF: VectorUDT):
    """
    This function calculates the log of the sum of the exponentiated
    logs taking out the max, i.e. insuring that the sum is not Inf
    """

    themax = np.max(logABF)
    result = themax + np.log(np.sum(np.exp(logABF - themax)))
    return float(result)


def getPosteriors(allAbfs: VectorUDT):
    """
    Calculates the posterior probability of each hypothesis given the evidence.
    """

    diff = allAbfs - getLogsum(allAbfs)
    abfsPosteriors = np.exp(diff)
    return Vectors.dense(abfsPosteriors)


def colocalisation(overlappingSignals, priorc1, priorc2, priorc12):
    """
    Compute Bayesian colocalisation analysis for all pairs of credible sets

    Args:
        overlappingSignals: DataFrame with overlapping signals
    """

    signalPairsCols = [
        "chrom",
        "studyKey",
        "lead_variant_id",
        "type",
    ]

    # register udfs
    logsum = F.udf(getLogsum, DoubleType())
    posteriors = F.udf(getPosteriors, VectorUDT())

    coloc = (
        overlappingSignals
        # Before summarizing logABF columns nulls need to be filled with 0:
        .fillna(0, subset=["left_logABF", "right_logABF"])
        # Sum of logABFs for each pair of signals
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
            # carrying over information and renaming columns (backwards compatible)
            F.first("left_study_id").alias("left_study"),
            F.first("left_phenotype_id").alias("left_phenotype"),
            F.first("left_bio_feature").alias("left_bio_feature"),
            F.first("left_lead_chrom").alias("left_chrom"),
            F.first("left_lead_pos").alias("left_pos"),
            F.first("left_lead_ref").alias("left_ref"),
            F.first("left_lead_alt").alias("left_alt"),
            F.first("right_study_id").alias("right_study"),
            F.first("right_phenotype_id").alias("right_phenotype"),
            F.first("right_bio_feature").alias("right_bio_feature"),
            F.first("right_lead_chrom").alias("right_chrom"),
            F.first("right_lead_pos").alias("right_pos"),
            F.first("right_lead_ref").alias("right_ref"),
            F.first("right_lead_alt").alias("right_alt"),
        )
        .withColumn("logsum1", logsum(F.col("left_logABF")))
        .withColumn("logsum2", logsum(F.col("right_logABF")))
        .withColumn("logsum12", logsum(F.col("sum_logABF")))
        .drop("left_logABF", "right_logABF", "sum_logABF")
        # Add priors
        # priorc1 Prior on variant being causal for trait 1
        .withColumn("priorc1", F.lit(priorc1))
        # priorc2 Prior on variant being causal for trait 2
        .withColumn("priorc2", F.lit(priorc2))
        # priorc12 Prior on variant being causal for traits 1 and 2
        .withColumn("priorc12", F.lit(priorc12))
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
    return coloc
