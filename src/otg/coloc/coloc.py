"""Utilities to perform colocalisation analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.ml.functions as fml
import pyspark.sql.functions as f
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
from pyspark.sql.types import DoubleType

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

from otg.coloc.overlaps import (
    find_all_vs_all_overlapping_signals,
    find_gwas_vs_all_overlapping_peaks,
)


def run_colocalisation(
    credible_sets: DataFrame,
    study_df: DataFrame,
    priorc1: float,
    priorc2: float,
    prioc12: float,
    pp_threshold: float,
    sumstats: DataFrame,
) -> DataFrame:
    """Run colocalisation analysis."""
    credible_sets_enriched = credible_sets.join(
        f.broadcast(study_df), on="studyId", how="left"
    ).persist()
    # 1. Standard colocalisation
    # Looking for overlapping signals
    overlapping_signals = find_all_vs_all_overlapping_signals(
        # We keep only the credible sets where probability is given as a bayes factor
        credible_sets_enriched.filter(f.col("logABF").isNotNull())
    )
    fm_coloc = colocalisation(overlapping_signals, priorc1, priorc2, prioc12)

    # 2. LD-expanded colocalisation
    # Looking for overlapping signals
    overlapping_signals = find_gwas_vs_all_overlapping_peaks(
        # We keep the credible sets where probability is given as a posterior probability (resulted from PICS and SuSIE for FINNGEN)
        credible_sets_enriched.filter(f.col("posteriorProbability") > pp_threshold),
        "posteriorProbability",
    )

    # IDEA: pass a parameter with the type of coloc: all vs all, gwas vs all
    metadata_cols = ["studyId", "phenotype", "biofeature"]
    ecaviar_coloc = (
        ecaviar_colocalisation(overlapping_signals, pp_threshold)
        # Add study metadata - to be deprecated
        # the resulting table has more rows because of the studies that have multiple mapped traits
        .join(
            study_df.selectExpr(*(f"{i} as left_{i}" for i in metadata_cols)),
            on="left_studyId",
            how="left",
        )
        .join(
            study_df.selectExpr(*(f"{i} as right_{i}" for i in metadata_cols)),
            on="right_studyId",
            how="left",
        )
        .distinct()
    )

    return (
        # 3. Join colocalisation results
        fm_coloc.unionByName(ecaviar_coloc, allowMissingColumns=True)
        # 4. Add betas from sumstats
        # Adds backwards compatibility with production schema
        # Note: First implementation in _add_coloc_sumstats_info hasn't been fully tested
        # .transform(lambda df: _add_coloc_sumstats_info(df, sumstats))
    )


def _get_logsum(log_abf: VectorUDT) -> float:
    """Calculates logsum of vector.

    This function calculates the log of the sum of the exponentiated
    logs taking out the max, i.e. insuring that the sum is not Inf

    Args:
        log_abf (VectorUDT): log approximate bayes factor

    Returns:
        float: logsum

    Example:
        >>> l = [0.2, 0.1, 0.05, 0]
        >>> round(_get_logsum(l), 6)
        1.476557
    """
    themax = np.max(log_abf)
    result = themax + np.log(np.sum(np.exp(log_abf - themax)))
    return float(result)


def _get_posteriors(all_abfs: VectorUDT) -> DenseVector:
    """Calculate posterior probabilities for each hypothesis.

    Args:
        all_abfs (VectorUDT): h0-h4 bayes factors

    Returns:
        DenseVector: Posterior
    """
    diff = all_abfs - _get_logsum(all_abfs)
    abfs_posteriors = np.exp(diff)
    return Vectors.dense(abfs_posteriors)


def _get_clpp(left_pp: Column, right_pp: Column) -> Column:
    """Calculate the colocalisation posterior probability (CLPP).

    If the fact that the same variant is found causal for two studies are independent events,
    CLPP is defined as the product of posterior porbabilities that a variant is causal in both studies.

    Args:
        left_pp (Column): left posterior probability
        right_pp (Column): right posterior probability

    Returns:
        Column: CLPP
    """
    return left_pp * right_pp


def colocalisation(
    overlapping_signals: DataFrame, priorc1: float, priorc2: float, priorc12: float
) -> DataFrame:
    """Calculate bayesian colocalisation based on overlapping signals.

    Args:
        overlapping_signals (DataFrame): overlapping peaks
        priorc1 (float): p1 prior
        priorc2 (float): p2 prior
        priorc12 (float): p12 prior

    Returns:
        DataFrame: Colocalisation results
    """
    signal_pairs_cols = [
        "chromosome",
        "studyId",
        "leadVariantId",
        "type",
    ]

    # register udfs
    logsum = f.udf(_get_logsum, DoubleType())
    posteriors = f.udf(_get_posteriors, VectorUDT())
    coloc = (
        overlapping_signals
        # Before summing log_abf columns nulls need to be filled with 0:
        .fillna(0, subset=["left_logABF", "right_logABF"])
        # Sum of log_abfs for each pair of signals
        .withColumn("sum_log_abf", f.col("left_logABF") + f.col("right_logABF"))
        # Group by overlapping peak and generating dense vectors of log_abf:
        .groupBy(
            *["left_" + col for col in signal_pairs_cols]
            + ["right_" + col for col in signal_pairs_cols]
        )
        .agg(
            f.count("*").alias("coloc_n_vars"),
            fml.array_to_vector(f.collect_list(f.col("left_logABF"))).alias(
                "left_logABF"
            ),
            fml.array_to_vector(f.collect_list(f.col("right_logABF"))).alias(
                "right_logABF"
            ),
            fml.array_to_vector(f.collect_list(f.col("sum_log_abf"))).alias(
                "sum_log_abf"
            ),
            # carrying over information and renaming columns (backwards compatible)
            f.first("left_phenotype").alias("left_phenotype"),
            f.first("left_biofeature").alias("left_biofeature"),
            f.first("left_gene_id").alias("left_gene_id"),
            f.first("right_phenotype").alias("right_phenotype"),
            f.first("right_biofeature").alias("right_biofeature"),
            f.first("right_gene_id").alias("right_gene_id"),
        )
        .withColumn("logsum1", logsum(f.col("left_logABF")))
        .withColumn("logsum2", logsum(f.col("right_logABF")))
        .withColumn("logsum12", logsum(f.col("sum_log_abf")))
        .drop("left_logABF", "right_logABF", "sum_log_abf")
        # Add priors
        # priorc1 Prior on variant being causal for trait 1
        .withColumn("priorc1", f.lit(priorc1))
        # priorc2 Prior on variant being causal for trait 2
        .withColumn("priorc2", f.lit(priorc2))
        # priorc12 Prior on variant being causal for traits 1 and 2
        .withColumn("priorc12", f.lit(priorc12))
        # h0-h2
        .withColumn("lH0abf", f.lit(0))
        .withColumn("lH1abf", f.log(f.col("priorc1")) + f.col("logsum1"))
        .withColumn("lH2abf", f.log(f.col("priorc2")) + f.col("logsum2"))
        # h3
        .withColumn("sumlogsum", f.col("logsum1") + f.col("logsum2"))
        # exclude null H3/H4s: due to sumlogsum == logsum12
        .filter(f.col("sumlogsum") != f.col("logsum12"))
        .withColumn("max", f.greatest("sumlogsum", "logsum12"))
        .withColumn(
            "logdiff",
            (
                f.col("max")
                + f.log(
                    f.exp(f.col("sumlogsum") - f.col("max"))
                    - f.exp(f.col("logsum12") - f.col("max"))
                )
            ),
        )
        .withColumn(
            "lH3abf",
            f.log(f.col("priorc1")) + f.log(f.col("priorc2")) + f.col("logdiff"),
        )
        .drop("right_logsum", "left_logsum", "sumlogsum", "max", "logdiff")
        # h4
        .withColumn("lH4abf", f.log(f.col("priorc12")) + f.col("logsum12"))
        # cleaning
        .drop("priorc1", "priorc2", "priorc12", "logsum1", "logsum2", "logsum12")
        # posteriors
        .withColumn(
            "allABF",
            fml.array_to_vector(
                f.array(
                    f.col("lH0abf"),
                    f.col("lH1abf"),
                    f.col("lH2abf"),
                    f.col("lH3abf"),
                    f.col("lH4abf"),
                )
            ),
        )
        .withColumn("posteriors", fml.vector_to_array(posteriors(f.col("allABF"))))
        .withColumn("coloc_h0", f.col("posteriors").getItem(0))
        .withColumn("coloc_h1", f.col("posteriors").getItem(1))
        .withColumn("coloc_h2", f.col("posteriors").getItem(2))
        .withColumn("coloc_h3", f.col("posteriors").getItem(3))
        .withColumn("coloc_h4", f.col("posteriors").getItem(4))
        .withColumn("coloc_h4_h3", f.col("coloc_h4") / f.col("coloc_h3"))
        .withColumn("coloc_log2_h4_h3", f.log2(f.col("coloc_h4_h3")))
        # clean up
        .drop("posteriors", "allABF", "lH0abf", "lH1abf", "lH2abf", "lH3abf", "lH4abf")
    )
    return coloc


def ecaviar_colocalisation(
    overlapping_signals: DataFrame, clpp_threshold: float
) -> DataFrame:
    """Calculate bayesian colocalisation based on overlapping signals.

    Args:
        overlapping_signals (DataFrame): DataFrame with overlapping signals.
        clpp_threshold (float): Colocalization cutoff threshold as described in the paper.

    Returns:
        DataFrame: DataFrame with colocalisation results.
    """
    signal_pairs_cols = [
        "chromosome",
        "studyId",
        "leadVariantId",
        "type",
    ]

    return (
        overlapping_signals.withColumn(
            "clpp",
            _get_clpp(
                f.col("left_posteriorProbability"), f.col("right_posteriorProbability")
            ),
        )
        .filter(f.col("clpp") > clpp_threshold)
        .groupBy(
            *(
                [f"left_{col}" for col in signal_pairs_cols]
                + [f"right_{col}" for col in signal_pairs_cols]
            )
        )
        .agg(
            f.count("*").alias("coloc_n_vars"),
            f.sum(f.col("clpp")).alias("clpp"),
        )
    )
