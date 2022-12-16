"""Harmonisation of GWAS stats."""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t
from scipy.stats import norm

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def _pval_to_zscore(pval_col: Column) -> Column:
    """Convert p-value column to z-score column.

    Args:
        pval_col (Column): pvalues to be casted to floats.

    Returns:
        Column: p-values transformed to z-scores

    Examples:
        >>> d = [{"id": "t1", "pval": "1"}, {"id": "t2", "pval": "0.9"}, {"id": "t3", "pval": "0.05"}, {"id": "t4", "pval": "1e-300"}, {"id": "t5", "pval": "1e-1000"}, {"id": "t6", "pval": "NA"}]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("zscore", _pval_to_zscore(f.col("pval"))).show()
        +---+-------+----------+
        | id|   pval|    zscore|
        +---+-------+----------+
        | t1|      1|       0.0|
        | t2|    0.9|0.12566137|
        | t3|   0.05|  1.959964|
        | t4| 1e-300| 37.537838|
        | t5|1e-1000| 37.537838|
        | t6|     NA|      null|
        +---+-------+----------+
        <BLANKLINE>

    """
    pvalue_float = pval_col.cast(t.FloatType())
    pvalue_nozero = f.when(pvalue_float == 0, sys.float_info.min).otherwise(
        pvalue_float
    )
    return f.udf(
        lambda pv: float(abs(norm.ppf((float(pv)) / 2))) if pv else None,
        t.FloatType(),
    )(pvalue_nozero)


def _get_reverse_complement(allele_col: Column) -> Column:
    """A function to return the reverse complement of an allele column.

    It takes a string and returns the reverse complement of that string if it's a DNA sequence,
    otherwise it returns the original string. Assumes alleles in upper case.

    Args:
        allele_col (Column): The column containing the allele to reverse complement.

    Returns:
        A column that is the reverse complement of the allele column.

    Examples:
        >>> d = [{"allele": 'A'}, {"allele": 'T'},{"allele": 'G'}, {"allele": 'C'},{"allele": 'AC'}, {"allele": 'GTaatc'},{"allele": '?'}, {"allele": None}]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("revcom_allele", _get_reverse_complement(f.col("allele"))).show()
        +------+-------------+
        |allele|revcom_allele|
        +------+-------------+
        |     A|            T|
        |     T|            A|
        |     G|            C|
        |     C|            G|
        |    AC|           GT|
        |GTaatc|       GATTAC|
        |     ?|            ?|
        |  null|         null|
        +------+-------------+
        <BLANKLINE>

    """
    allele_col = f.upper(allele_col)
    return f.when(
        allele_col.rlike("[ACTG]+"),
        f.reverse(f.translate(allele_col, "ACTG", "TGAC")),
    ).otherwise(allele_col)


def _harmonize_beta(
    effect_size: Column, confidence_interval: Column, needs_harmonization: Column
) -> Column:
    """A function to harmonize beta.

    If the confidence interval contains the word "increase" or "decrease" it indicates, we are dealing with betas.
    If it's "increase" and the effect size needs to be harmonized,
    then multiply the effect size by -1

    Args:
        effect_size (Column): The effect size column from the dataframe
        confidence_interval (Column): The confidence interval of the effect size.
        needs_harmonization (Column): a boolean column that indicates whether the effect size needs to be flipped

    Returns:
        A column of the harmonized beta values.

    Examples:
        >>> d = [
        ...     {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9] unit decrease', 'needs_harmonization': True},
        ...     {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9] unit decrease', 'needs_harmonization': False},
        ...     {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9] unit increase', 'needs_harmonization': True},
        ...     {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9] unit increase', 'needs_harmonization': False},
        ...     {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9]', 'needs_harmonization': False},
        ... ]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("beta", _harmonize_beta(f.col("effect_size"), f.col('confidence_interval'), f.col('needs_harmonization'))).show()
        +--------------------+-----------+-------------------+----+
        | confidence_interval|effect_size|needs_harmonization|beta|
        +--------------------+-----------+-------------------+----+
        |[0.1-0.9] unit de...|        0.6|               true| 0.6|
        |[0.1-0.9] unit de...|        0.6|              false|-0.6|
        |[0.1-0.9] unit in...|        0.6|               true|-0.6|
        |[0.1-0.9] unit in...|        0.6|              false| 0.6|
        |           [0.1-0.9]|        0.6|              false|null|
        +--------------------+-----------+-------------------+----+
        <BLANKLINE>

    """
    # The effect is given as beta, if the confidence interval contains 'increase' or 'decrease'
    beta = f.when(
        confidence_interval.contains("increase")
        | confidence_interval.contains("decrease"),
        effect_size,
    )
    # Flipping beta if harmonization is required or effect negated by saying 'decrease'
    return f.when(
        (confidence_interval.contains("increase") & needs_harmonization)
        | (confidence_interval.contains("decrease") & ~needs_harmonization),
        beta * -1,
    ).otherwise(beta)


def _calculate_beta_ci(beta: Column, zscore: Column, direction: Column) -> Column:
    """Calculating confidence intervals for beta values.

    Args:
        beta (Column): The beta value for the given row
        zscore (Column): The z-score of the beta coefficient.
        direction (Column): This is the direction of the confidence interval. It can be either "upper" or "lower".

    Returns:
        The upper and lower bounds of the confidence interval for the beta coefficient.

    Examples:
        >>> d = [
        ...    {"beta": 0.6, 'zscore': 3, 'direction': 'upper'},
        ...    {"beta": 0.6, 'zscore': 3, 'direction': 'lower'},
        ...    {"beta": 0.6, 'zscore': 3, 'direction': 'something'},
        ...    {"beta": None, 'zscore': 3, 'direction': 'lower'},
        ... ]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("beta_confidence_interval",  _calculate_beta_ci(f.col("beta"), f.col('zscore'), f.col('direction'))).show()
        +----+---------+------+------------------------+
        |beta|direction|zscore|beta_confidence_interval|
        +----+---------+------+------------------------+
        | 0.6|    upper|     3|                   0.992|
        | 0.6|    lower|     3|     0.20800000000000002|
        | 0.6|something|     3|                    null|
        |null|    lower|     3|                    null|
        +----+---------+------+------------------------+
        <BLANKLINE>

    """
    zscore_95 = f.lit(1.96)
    return (
        f.when(direction == "upper", beta + f.abs(zscore_95 * beta) / zscore)
        .when(direction == "lower", beta - f.abs(zscore_95 * beta) / zscore)
        .otherwise(None)
    )


def _harmonize_odds_ratio(
    effect_size: Column, confidence_interval: Column, needs_harmonization: Column
) -> Column:
    """Harmonizing odds ratio.

    Args:
        effect_size (Column): The effect size column from the dataframe
        confidence_interval (Column): The confidence interval of the effect size.
        needs_harmonization (Column): a boolean column that indicates whether the odds ratio needs to be flipped

    Returns:
        A column with the odds ratio, or 1/odds_ratio if harmonization required.

    Examples:
        >>> d = [
        ...   {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9] unit decrease', 'needs_harmonization': True},
        ...   {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9]', 'needs_harmonization': False},
        ...   {"effect_size": 0.6, 'confidence_interval': '[0.1-0.9]', 'needs_harmonization': True},
        ... ]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("odds_ratio", _harmonize_odds_ratio(f.col("effect_size"), f.col('confidence_interval'), f.col('needs_harmonization'))).show()
        +--------------------+-----------+-------------------+------------------+
        | confidence_interval|effect_size|needs_harmonization|        odds_ratio|
        +--------------------+-----------+-------------------+------------------+
        |[0.1-0.9] unit de...|        0.6|               true|              null|
        |           [0.1-0.9]|        0.6|              false|               0.6|
        |           [0.1-0.9]|        0.6|               true|1.6666666666666667|
        +--------------------+-----------+-------------------+------------------+
        <BLANKLINE>

    """
    # The confidence interval tells if we are not dealing with betas -> OR
    odds_ratio = f.when(
        ~confidence_interval.contains("increase")
        & ~confidence_interval.contains("decrease"),
        effect_size,
    )

    return f.when(needs_harmonization, 1 / odds_ratio).otherwise(odds_ratio)


def _calculate_or_ci(odds_ratio: Column, zscore: Column, direction: Column) -> Column:
    """Calculating confidence intervals for odds-ratio values.

    Args:
        odds_ratio (Column): The odds ratio of the association between the exposure and outcome.
        zscore (Column): The z-score of the confidence interval.
        direction (Column): This is either "upper" or "lower" and determines whether the upper or lower confidence interval is calculated.

    Returns:
        The upper and lower bounds of the 95% confidence interval for the odds ratio.

    Examples:
        >>> d = [
        ...     {"odds_ratio": 0.6, 'zscore': 3, 'direction': 'upper'},
        ...     {"odds_ratio": 1.6, 'zscore': 3, 'direction': 'lower'},
        ...     {"odds_ratio": 0.6, 'zscore': 3, 'direction': 'something'},
        ...     {"odds_ratio": None, 'zscore': 3, 'direction': 'lower'},
        ... ]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("or_confidence_interval", _calculate_or_ci(f.col("odds_ratio"), f.col('zscore'), f.col('direction'))).show()
        +---------+----------+------+----------------------+
        |direction|odds_ratio|zscore|or_confidence_interval|
        +---------+----------+------+----------------------+
        |    upper|       0.6|     3|    0.8377075574145849|
        |    lower|       1.6|     3|    1.1769597039688107|
        |something|       0.6|     3|                  null|
        |    lower|      null|     3|                  null|
        +---------+----------+------+----------------------+
        <BLANKLINE>

    """
    zscore_95 = f.lit(1.96)
    odds_ratio_estimate = f.log(odds_ratio)
    odds_ratio_se = odds_ratio_estimate / zscore
    return f.when(
        direction == "upper",
        f.exp(odds_ratio_estimate + f.abs(zscore_95 * odds_ratio_se)),
    ).when(
        direction == "lower",
        f.exp(odds_ratio_estimate - f.abs(zscore_95 * odds_ratio_se)),
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
        # If the alleles are palindrome - the reference and alt alleles are reverse complement of each other:
        # eg. T -> A: in such cases we cannot disambiguate the effect, which means we cannot be sure if
        # the effect is given to the alt allele on the positive strand or the ref allele on
        # The negative strand. We assume, we don't need to harminze.
        # Adding a flag indicating if effect harmonization is required:
        .withColumn(
            "isPalindrome",
            f.when(
                f.col("referenceAllele")
                == _get_reverse_complement(f.col("alternateAllele")),
                True,
            ).otherwise(False),
        )
        # If a variant is palindrome, we drop the effect size, so no harmonization can happen:
        .withColumn(
            "effectSize",
            f.when(f.col("isPalindrome"), None).otherwise(f.col("effectSize")),
        )
        # As we are calculating effect on the alternate allele, we have to harmonise effect
        # if the risk allele is reference allele or the reverse complement of the reference allele
        .withColumn(
            "needsHarmonisation",
            f.when(
                (f.col("riskAllele") == f.col("referenceAllele"))
                | (
                    f.col("riskAllele")
                    == _get_reverse_complement(f.col("referenceAllele"))
                ),
                True,
            ).otherwise(False),
        )
        # Z-score is needed to calculate 95% confidence interval:
        .withColumn(
            "zscore",
            _pval_to_zscore(
                f.concat_ws("E", f.col("pValueMantissa"), f.col("pValueExponent"))
            ),
        )
        # Harmonizing betas + calculate the corresponding confidence interval:
        .withColumn(
            "beta",
            _harmonize_beta(
                f.col("effectSize"),
                f.col("confidenceInterval"),
                f.col("needsHarmonisation"),
            ),
        )
        .withColumn(
            "beta_ci_upper",
            _calculate_beta_ci(f.col("beta"), f.col("zscore"), f.lit("upper")),
        )
        .withColumn(
            "beta_ci_lower",
            _calculate_beta_ci(f.col("beta"), f.col("zscore"), f.lit("lower")),
        )
        # Harmonizing odds-ratios + calculate the corresponding confidence interval:
        .withColumn(
            "odds_ratio",
            _harmonize_odds_ratio(
                f.col("effectSize"),
                f.col("confidenceInterval"),
                f.col("needsHarmonisation"),
            ),
        )
        .withColumn(
            "odds_ratio_ci_upper",
            _calculate_or_ci(f.col("odds_ratio"), f.col("zscore"), f.lit("upper")),
        )
        .withColumn(
            "odds_ratio_ci_lower",
            _calculate_or_ci(f.col("odds_ratio"), f.col("zscore"), f.lit("lower")),
        )
        # Adding QC flag to variants with palindrome alleles:
        .withColumn(
            "qualityControl",
            f.when(
                f.col("isPalindrome"),
                f.array_union(
                    f.col("qualityControl"), f.array(f.lit("Palindrome alleles"))
                ),
            ).otherwise(f.col("qualityControl")),
        )
        # Dropping unused columns:
        .drop(
            "zscore",
            "effectSize",
            "confidenceInterval",
            "needsHarmonisation",
            "isPalindrome",
            "zscore",
            "riskAllele",
        )
        .persist()
    )
