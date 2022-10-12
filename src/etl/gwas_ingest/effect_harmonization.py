"""Harmonisation of GWAS stats."""
from __future__ import annotations

import sys
from statistics import NormalDist
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def pval_to_zscore(df: DataFrame, pvalcol: str) -> DataFrame:
    """Convert p-value to z-score.

    Args:
        df (DataFrame): input DataFrame
        pvalcol (str): name of the p-value column

    Returns:
        DataFrame: Input DataFrame with an extra `zscore` column

    Examples:
        >>> d = [{"id": "t1", "pval": "0.05"},{"id": "t2", "pval": "1e-300"}, {"id": "t3", "pval": "0.9"}]
        >>> df = spark.createDataFrame(d)
        >>> pval_to_zscore(df, "pval").show()
        +---+------+-----------+
        | id|  pval|     zscore|
        +---+------+-----------+
        | t1|  0.05|0.062706776|
        | t2|1e-300|        0.0|
        | t3|   0.9|  1.6448535|
        +---+------+-----------+
        <BLANKLINE>
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
                lambda pv: NormalDist().inv_cdf((1 + pv) / 2.0) if pv else None,
                t.FloatType(),
            )(f.col(f"parsed_${pvalcol}")),
        )
        # Dropping helper column:
        .drop(f"parsed_${pvalcol}")
    )


def get_reverse_complement(df: DataFrame, allele_col: str) -> DataFrame:
    """Get reverse complement allele of a specified allele column.

    Args:
        df (DataFrame): input DataFrame
        allele_col (str): the name of the column containing the allele

    Returns:
        DataFrame: A dataframe with a new column called revcomp_{allele_col}
    """
    return df.withColumn(
        f"revcomp_{allele_col}",
        f.when(
            f.col(allele_col).rlike("[ACTG]+"),
            f.reverse(f.translate(f.col(allele_col), "ACTG", "TGAC")),
        ),
    )


def harmonise_beta(df: DataFrame) -> DataFrame:
    """Harmonise betas.

    The harmonization of the beta follows the logic:
    - The beta is flipped (multiplied by -1) if:
        1) the effect needs harmonization and
        2) the annotation of the effect is annotated as decrease
    - The 95% confidence interval of the effect is calculated using the z-score
    - Irrelevant columns are dropped.

    Args:
        df (DataFrame): summary stat DataFrame

    Returns:
        DataFrame: input DataFrame with harmonised beta columns:
            - beta
            - beta_ci_lower
            - beta_ci_upper
            - beta_direction
    """
    # The z-score corresponding to p-value: 0.05
    zscore_95 = 1.96

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
                f.col("beta") - f.lit(zscore_95) * f.col("beta") / f.col("zscore"),
                f.col("beta") + f.lit(zscore_95) * f.col("beta") / f.col("zscore"),
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
    """Harmonise odds ratio.

    The harmonization of the odds ratios follows the logic:
    - The effect is flipped (reciprocal value is calculated) if the effect needs harmonization
    - The 95% confidence interval is calculated using the z-score
    - Irrelevant columns are dropped.

    Args:
        df (DataFrame): summary stat DataFrame

    Returns:
        DataFrame: odds ratio with harmonised OR in columns:
            - odds_ratio
            - odds_ratio_ci_lower
            - odds_ratio_ci_upper
            - odds_ratio_direction
    """
    # The z-score corresponding to p-value: 0.05
    zscore_95 = 1.96

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
                    f.col("odds_ratio_estimate")
                    - f.lit(zscore_95) * f.col("odds_ratio_se")
                ),
                f.exp(
                    f.col("odds_ratio_estimate")
                    + f.lit(zscore_95) * f.col("odds_ratio_se")
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
    """Harmonisation of effects.

    Args:
        df (DataFrame): GWASCatalog stats

    Returns:
        DataFrame: Harmonised GWASCatalog stats
    """
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
