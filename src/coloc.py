"""
Utilities to perform colocalisation analysis
"""

from pyspark.ml.linalg import VectorUDT, Vectors
import numpy as np
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
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

    # register udfs
    logsum = F.udf(getLogsum, T.DoubleType())
    posteriors = F.udf(getPosteriors, VectorUDT())

    signalPairsCols = [
        "studyKey",
        "lead_variant_id",
        "type",
        # "study_id",
        # "phenotype_id",
        # "bio_feature",
        # "lead_chrom",
        # "lead_pos",
        # "lead_ref",
        # "lead_alt",
    ]

    windowSpec = Window.partitionBy(
        ["left_" + col for col in signalPairsCols]
        + ["right_" + col for col in signalPairsCols]
    ).orderBy("tag_variant_id")
    windowSpecAgg = Window.partitionBy(
        ["left_" + col for col in signalPairsCols]
        + ["right_" + col for col in signalPairsCols]
    )
    coloc = (
        overlappingSignals
        # Before summarizing logABF columns nulls need to be filled with 0:
        .fillna(0, subset=["left_logABF", "right_logABF"])
        # Grouping data by peak and collect list of the sums:
        .withColumn("sum_logABF", F.col("left_logABF") + F.col("right_logABF"))
        # Group by overlapping peak and generating dense vectors of logABF:
        .withColumn("row_number", F.row_number().over(windowSpec))
        .withColumn("coloc_n_vars", F.count("tag_variant_id").over(windowSpecAgg))
        .withColumn(
            "left_logABF",
            Fml.array_to_vector(
                F.collect_list(F.col("left_logABF")).over(windowSpecAgg)
            ),
        )
        .withColumn(
            "right_logABF",
            Fml.array_to_vector(
                F.collect_list(F.col("right_logABF")).over(windowSpecAgg)
            ),
        )
        .withColumn(
            "sum_logABF",
            Fml.array_to_vector(
                F.collect_list(F.col("sum_logABF")).over(windowSpecAgg)
            ),
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        # Log sums
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
