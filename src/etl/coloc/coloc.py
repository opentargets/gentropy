"""Utilities to perform colocalisation analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.ml.functions as fml
import pyspark.sql.functions as f
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
from pyspark.sql.types import DoubleType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from etl.coloc.overlaps import find_all_vs_all_overlapping_signals
from etl.coloc.utils import _add_moleculartrait_phenotype_genes


def run_colocalisation(
    credible_sets: DataFrame,
    study_df: DataFrame,
    priorc1: float,
    priorc2: float,
    prioc12: float,
    phenotype_id_gene: DataFrame,
    sumstats: DataFrame,
) -> None:
    """Run colocalisation analysis."""
    # 1. Looking for overlapping signals
    overlapping_signals = find_all_vs_all_overlapping_signals(credible_sets, study_df)

    # 2. Perform colocalisation analysis
    coloc = colocalisation(
        overlapping_signals,
        priorc1,
        priorc2,
        prioc12,
    )

    # 3. Add molecular trait genes (metadata)
    coloc_with_genes = _add_moleculartrait_phenotype_genes(coloc, phenotype_id_gene)

    # 4. Add betas from sumstats
    # Adds backwards compatibility with production schema
    # Note: First implementation in _add_coloc_sumstats_info hasn't been fully tested
    # colocWithAllMetadata = _add_coloc_sumstats_info(coloc_with_genes, sumstats)

    return coloc_with_genes


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
        "studyKey",
        "leadVariantId",
        "type",
    ]

    # register udfs
    logsum = f.udf(_get_logsum, DoubleType())
    posteriors = f.udf(_get_posteriors, VectorUDT())
    coloc = (
        overlapping_signals
        # Before summarizing log_abf columns nulls need to be filled with 0:
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
            f.first("left_studyId").alias("left_study"),
            f.first("left_traitFromSourceMappedId").alias("left_phenotype"),
            f.first("left_biofeature").alias("left_biofeature"),
            f.first("right_studyId").alias("right_study"),
            f.first("right_traitFromSourceMappedId").alias("right_phenotype"),
            f.first("right_biofeature").alias("right_biofeature"),
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
