from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import scipy.stats as st
from pyspark.sql import functions as f
from pyspark.sql import types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def pval_to_zscore(df: DataFrame, pvalcol: str) -> DataFrame:
    """
    This function converts a p-value to a z-score

    Args:
      df (DataFrame): DataFrame
      pvalcol (str): The name of the column containing the p-values.

    Returns:
      A dataframe with a 'zscore' column.
    """
    return (
        df
        # Converting string p-values to float:
        .withColumn(
            f"parsed_${pvalcol}", f.col(pvalcol).cast(t.FloatType())
        ).withColumn(
            f"parsed_${pvalcol}",
            f.when(f.col(f"parsed_${pvalcol}") == 0, sys.float_info.min).otherwise(
                f.col(f"parsed_${pvalcol}")
            ),
        )
        # Convert pvalues to z-score:
        .withColumn(
            "zscore",
            f.udf(
                lambda pv: float(abs(st.norm.ppf(pv / 2))) if pv else None,
                t.FloatType(),
            )(f.col(f"parsed_${pvalcol}")),
        )
        # Dropping helper column:
        .drop(f"parsed_${pvalcol}")
    )


def get_reverse_complement(df: DataFrame, allele_col: str) -> DataFrame:
    """
    This function returns a data frame with an additional column containing the reverse complement allele of a specified allele column

    Args:
      df (DataFrame): DataFrame
      allele_col (str): the name of the column containing the allele

    Returns:
      A dataframe with a new column called revcomp_{allele_col}
    """

    return df.withColumn(
        f"revcomp_{allele_col}",
        f.when(
            f.col(allele_col).rlike("[ACTG]+"),
            f.reverse(f.translate(f.col(allele_col), "ACTG", "TGAC")),
        ),
    )


def harmonise_beta(df: DataFrame) -> DataFrame:
    """
    The harmonization of the beta follows the logic:
    - The beta is flipped (multiplied by -1) if:
        1) the effect needs harmonization and
        2) the annotation of the effect is annotated as decrease
    - The 95% confidence interval of the effect is calculated using the z-score
    - Irrelevant columns are dropped.


    The function returns a dataframe with the following columns:

    - beta
    - beta_ci_lower
    - beta_ci_upper
    - beta_direction

    Args:
      df (DataFrame): DataFrame
    """

    return (
        df.withColumn(
            "beta",
            f.when(
                (
                    f.col("confidence_interval").contains("increase")
                    & f.col("needs_harmonization")
                )
                | (
                    f.col("confidence_interval").contains("decrease")
                    & ~f.col("needs_harmonization")
                ),
                f.col("beta") * -1,
            ).otherwise(f.col("beta")),
        )
        .withColumn(
            "beta_conf_intervals",
            f.array(
                f.col("beta") - f.lit(1.96) * f.col("beta") / f.col("zscore"),
                f.col("beta") + f.lit(1.96) * f.col("beta") / f.col("zscore"),
            ),
        )
        .withColumn("beta_ci_lower", f.array_min(f.col("beta_conf_intervals")))
        .withColumn("beta_ci_upper", f.array_max(f.col("beta_conf_intervals")))
        .withColumn(
            "beta_direction",
            f.when(f.col("beta") >= 0, "+").when(f.col("beta") < 0, "-"),
        )
        .drop("beta_conf_intervals")
    )


def harmonise_odds_ratio(df: DataFrame) -> DataFrame:
    """
    The harmonization of the odds ratios follows the logic:
    - The effect is flipped (reciprocal value is calculated) if the effect needs harmonization
    - The 95% confidence interval is calculated using the z-score
    - Irrelevant columns are dropped.

    The function returns a dataframe with the following columns:

    - odds_ratio
    - odds_ratio_ci_lower
    - odds_ratio_ci_upper
    - odds_ratio_direction

    Args:
        df (DataFrame): DataFrame

    Returns:
        A dataframe with the odds ratio harmonized.
    """
    return (
        df.withColumn(
            "odds_ratio",
            f.when(f.col("needs_harmonization"), 1 / f.col("odds_ratio")).otherwise(
                f.col("odds_ratio")
            ),
        )
        .withColumn("odds_ratio_estimate", f.log(f.col("odds_ratio")))
        .withColumn("odds_ratio_se", f.col("odds_ratio_estimate") / f.col("zscore"))
        .withColumn(
            "odds_ratio_direction",
            f.when(f.col("odds_ratio") >= 1, "+").when(f.col("odds_ratio") < 1, "-"),
        )
        .withColumn(
            "odds_ratio_conf_intervals",
            f.array(
                f.exp(
                    f.col("odds_ratio_estimate") - f.lit(1.96) * f.col("odds_ratio_se")
                ),
                f.exp(
                    f.col("odds_ratio_estimate") + f.lit(1.96) * f.col("odds_ratio_se")
                ),
            ),
        )
        .withColumn(
            "odds_ratio_ci_lower", f.array_min(f.col("odds_ratio_conf_intervals"))
        )
        .withColumn(
            "odds_ratio_ci_upper", f.array_max(f.col("odds_ratio_conf_intervals"))
        )
        .drop("odds_ratio_conf_intervals", "odds_ratio_se", "odds_ratio_estimate")
    )


def harmonize_effect(df: DataFrame) -> DataFrame:
    return (
        df
        # Get reverse complement of the alleles of the mapped variants:
        .transform(lambda df: get_reverse_complement(df, "alt"))
        .transform(lambda df: get_reverse_complement(df, "ref"))
        # A variant is palindromic if the reference and alt alleles are reverse complement of each other:
        # eg. T -> A: in such cases we cannot disambigate the effect, which means we cannot be sure if
        # the effect is given to the alt allele on the positive strand or the ref allele on
        # The negative strand.
        .withColumn(
            "is_palindrome",
            f.when(f.col("ref") == f.col("revcomp_alt"), True).otherwise(False),
        )
        # We are harmonizing the effect on the alternative allele:
        # Adding a flag to trigger harmonization if: risk == ref or risk == revcomp(ref):
        .withColumn(
            "needs_harmonization",
            f.when(
                (f.col("risk_allele") == f.col("ref"))
                | (f.col("risk_allele") == f.col("revcomp_ref")),
                True,
            ).otherwise(False),
        )
        # Z-score is needed to calculate 95% confidence interval:
        .transform(lambda df: pval_to_zscore(df, "p_value"))
        # Annotation provides information if the effect is odds-ratio or beta:
        # Effect is lost for variants with palindromic alleles.
        .withColumn(
            "beta",
            f.when(
                f.col("confidence_interval").rlike(r"[increase|decrease]")
                & (~f.col("is_palindrome")),
                f.col("or_beta"),
            ),
        )
        .withColumn(
            "odds_ratio",
            f.when(
                (~f.col("confidence_interval").rlike(r"[increase|decrease]"))
                & (~f.col("is_palindrome")),
                f.col("or_beta"),
            ),
        )
        # Harmonize beta:
        .transform(harmonise_beta)
        # Harmonize odds-ratio:
        .transform(harmonise_odds_ratio)
        # Coalesce effect direction:
        .withColumn(
            "direction",
            f.coalesce(f.col("beta_direction"), f.col("odds_ratio_direction")),
        )
    )
