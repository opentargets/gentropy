"""Statistic calculations."""

from __future__ import annotations

import sys
from math import floor, log10
from typing import TYPE_CHECKING

import numpy as np
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.types import GWASEffect, PValComponents
from gentropy.common.udf import chi2_inverse_survival_function, chi2_survival_function

if TYPE_CHECKING:
    from numpy.typing import NDArray

###################### PYTHON FUNCTION #######################


def get_logsum(arr: NDArray[np.float64]) -> float:
    """Calculates logarithm of the sum of exponents of a vector. The max is extracted to ensure that the sum is not Inf.

    This function emulates scipy's logsumexp expression.

    Args:
        arr (NDArray[np.float64]): input array

    Returns:
        float: logsumexp of the input array

    Examples:
        >>> l = [0.2, 0.1, 0.05, 0]
        >>> round(get_logsum(l), 6)
        1.476557
    """
    MAX = np.max(arr)
    result = MAX + np.log(np.sum(np.exp(arr - MAX)))
    return float(result)


def split_pvalue(pvalue: float) -> tuple[float, int]:
    """Convert a float to 10 based exponent and mantissa.

    Args:
        pvalue (float): p-value

    Returns:
        tuple[float, int]: Tuple with mantissa and exponent

    Raises:
        ValueError: If p-value is not between 0 and 1

    Examples:
        >>> split_pvalue(0.00001234)
        (1.234, -5)

        >>> split_pvalue(1)
        (1.0, 0)

        >>> split_pvalue(0.123)
        (1.23, -1)

        >>> split_pvalue(0.99)
        (9.9, -1)
    """
    if pvalue < 0.0 or pvalue > 1.0:
        raise ValueError("P-value must be between 0 and 1")

    exponent = floor(log10(pvalue)) if pvalue != 0 else 0
    mantissa = round(pvalue / 10**exponent, 3)
    return (mantissa, exponent)


##################### SPARK FUNCTION #######################


def chi2_from_pvalue(p_value_mantissa: Column, p_value_exponent: Column) -> Column:
    """Calculate chi2 from p-value.

    This function calculates the chi2 value from the p-value mantissa and exponent.
    In case the p-value is very small (exponent < -300), it uses an approximation based on a linear regression model.
    The approximation is based on the formula: -5.367 * neglog_pval + 4.596, where neglog_pval is the negative log10 of the p-value mantissa.


    Args:
        p_value_mantissa (Column): Mantissa of the p-value (float)
        p_value_exponent (Column): Exponent of the p-value (integer)

    Returns:
        Column: Chi2 value (float)

    Examples:
        >>> data = [(5.0, -8), (9.0, -300), (9.0, -301)]
        >>> schema = "pValueMantissa FLOAT, pValueExponent INT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +--------------+--------------+
        |pValueMantissa|pValueExponent|
        +--------------+--------------+
        |           5.0|            -8|
        |           9.0|          -300|
        |           9.0|          -301|
        +--------------+--------------+
        <BLANKLINE>

        >>> mantissa = f.col("pValueMantissa")
        >>> exponent = f.col("pValueExponent")
        >>> chi2 = f.round(chi2_from_pvalue(mantissa, exponent), 2).alias("chi2")
        >>> df2 = df.select(mantissa, exponent, chi2)
        >>> df2.show()
        +--------------+--------------+-------+
        |pValueMantissa|pValueExponent|   chi2|
        +--------------+--------------+-------+
        |           5.0|            -8|  29.72|
        |           9.0|          -300|1369.48|
        |           9.0|          -301|1373.64|
        +--------------+--------------+-------+
        <BLANKLINE>
    """
    PVAL_EXP_THRESHOLD = f.lit(-300)
    APPROX_INTERCEPT = f.lit(-5.367)
    APPROX_COEF = f.lit(4.596)
    neglog_pval = neglogpval_from_pvalue(p_value_mantissa, p_value_exponent)
    p_value = p_value_mantissa * f.pow(10, p_value_exponent)
    neglog_approx = (neglog_pval * APPROX_COEF + APPROX_INTERCEPT).cast(t.DoubleType())

    return (
        f.when(p_value_exponent < PVAL_EXP_THRESHOLD, neglog_approx)
        .otherwise(chi2_inverse_survival_function(p_value))
        .alias("chi2")
    )


def ci(
    pvalue_mantissa: Column,
    pvalue_exponent: Column,
    beta: Column,
    standard_error: Column,
) -> tuple[Column, Column]:
    """Calculate the confidence interval for the effect based on the p-value and the effect size.

    If the standard error already available, don't re-calculate from p-value.

    Args:
        pvalue_mantissa (Column): p-value mantissa (float)
        pvalue_exponent (Column): p-value exponent (integer)
        beta (Column): effect size in beta (float)
        standard_error (Column): standard error.

    Returns:
        tuple[Column, Column]: betaConfidenceIntervalLower (float), betaConfidenceIntervalUpper (float)

    Examples:
        >>> df = spark.createDataFrame([
        ...     (2.5, -10, 0.5, 0.2),
        ...     (3.0, -5, 1.0, None),
        ...     (1.5, -8, -0.2, 0.1)
        ...     ], ["pvalue_mantissa", "pvalue_exponent", "beta", "standard_error"]
        ... )
        >>> df.select("*", *ci(f.col("pvalue_mantissa"), f.col("pvalue_exponent"), f.col("beta"), f.col("standard_error"))).show()
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        |pvalue_mantissa|pvalue_exponent|beta|standard_error|betaConfidenceIntervalLower|betaConfidenceIntervalUpper|
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        |            2.5|            -10| 0.5|           0.2|        0.10799999999999998|                      0.892|
        |            3.0|             -5| 1.0|          NULL|         0.5303664052547075|         1.4696335947452925|
        |            1.5|             -8|-0.2|           0.1|                     -0.396|       -0.00400000000000...|
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        <BLANKLINE>
    """
    # Calculate p-value from mantissa and exponent:
    pvalue = pvalue_mantissa * f.pow(10, pvalue_exponent)

    # Fix p-value underflow:
    pvalue = f.when(pvalue == 0, sys.float_info.min).otherwise(pvalue)

    # Compute missing standard error:
    standard_error = f.when(
        standard_error.isNull(), f.abs(beta) / f.abs(zscore_from_pvalue(pvalue, beta))
    ).otherwise(standard_error)

    # Calculate upper and lower confidence interval:
    z_score_095 = 1.96
    ci_lower = (beta - z_score_095 * standard_error).alias(
        "betaConfidenceIntervalLower"
    )
    ci_upper = (beta + z_score_095 * standard_error).alias(
        "betaConfidenceIntervalUpper"
    )

    return (ci_lower, ci_upper)


def neglogpval_from_z2(z2: Column) -> Column:
    """Calculate negative log10 of p-value from squared Z-score following chi2 distribution.

    **The Z-score^2 is equal to the chi2 with 1 degree of freedom.**

    In case of very large Z-score (very small corresponding p-value), the function uses a linear approximation.

    Args:
        z2 (Column): Z-score squared.

    Returns:
        Column:  negative log of p-value.

    Examples:
        >>> data = [(1.0,), (2000.0,)]
        >>> schema = "z2 FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +------+
        |    z2|
        +------+
        |   1.0|
        |2000.0|
        +------+
        <BLANKLINE>

        >>> neglogpval = f.round(neglogpval_from_z2(f.col("z2")), 2).alias("neglogpval")
        >>> df2 = df.select(f.col("z2"), neglogpval)
        >>> df2.show()
        +------+----------+
        |    z2|neglogpval|
        +------+----------+
        |   1.0|       0.5|
        |2000.0|    436.02|
        +------+----------+
        <BLANKLINE>
    """
    MAX_EXACT_Z2 = f.lit(1400)
    APPROX_INTERCEPT = f.lit(1.4190)
    APPROX_COEFF = f.lit(0.2173)
    approximate_neglogpval_from_z2 = APPROX_INTERCEPT + APPROX_COEFF * z2
    computed_neglogpval_from_z2 = -1 * f.log10(chi2_survival_function(z2))
    return f.when(z2 <= MAX_EXACT_Z2, computed_neglogpval_from_z2).otherwise(
        approximate_neglogpval_from_z2
    )


def neglogpval_from_pvalue(
    p_value_mantissa: Column, p_value_exponent: Column
) -> Column:
    """Compute the negative log p-value.

    Args:
        p_value_mantissa (Column): P-value mantissa
        p_value_exponent (Column): P-value exponent

    Returns:
        Column: Negative log p-value

    Examples:
        >>> d = [(1, 1), (5, -2), (1, -1000)]
        >>> df = spark.createDataFrame(d).toDF("p_value_mantissa", "p_value_exponent")
        >>> df.withColumn("neg_log_p", neglogpval_from_pvalue(f.col("p_value_mantissa"), f.col("p_value_exponent"))).show()
        +----------------+----------------+------------------+
        |p_value_mantissa|p_value_exponent|         neg_log_p|
        +----------------+----------------+------------------+
        |               1|               1|              -1.0|
        |               5|              -2|1.3010299956639813|
        |               1|           -1000|            1000.0|
        +----------------+----------------+------------------+
        <BLANKLINE>
    """
    return -1 * (f.log10(p_value_mantissa) + p_value_exponent)


def normalise_gwas_statistics(
    beta: Column,
    odds_ratio: Column,
    standard_error: Column,
    ci_upper: Column,
    ci_lower: Column,
    mantissa: Column,
    exponent: Column,
) -> GWASEffect:
    """Normalise beta and standard error from given values.

    This function attempts to harmonise Effect and Standard Error given various inputs.

    Note:
        Effect (Beta) harmonisation:
        - If beta is not null, it is kept as is.
        - If beta is null, but odds ratio is not null, odds ratio is converted to beta

    Note:
        Effect Standard Error (std(beta)) harmonisation
        **Prefer calculation from p-value and beta, if available, as the confidence interval is usually rounded and may lead to loss of precision**:
        - If standard error is not null, it is kept as is.
        - If standard error is null, but beta, pval-mantissa, pval-exponent are not null, convert pval components and beta to standard error
        - If standard error is null, but ci-upper and ci-lower are not null and they come from odds ratio, convert them to standard error.


    Args:
        beta (Column): Effect in beta.
        odds_ratio (Column): Effect in odds ratio.
        standard_error (Column): Standard error of the effect.
        ci_upper (Column): Upper bound of the confidence interval.
        ci_lower (Column): Lower bound of the confidence interval.
        mantissa (Column): Mantissa of the p-value.
        exponent (Column): Exponent of the p-value.

    Returns:
        GWASEffect: named tuple with standardError and beta columns.

    Examples:
        >>> x1 = (0.1, 1.1, 0.1, None, None, 9.0, -100) # keep beta, keep std error
        >>> x2 = (None, 1.1, 0.1, None, None, 9.0, -100) # convert odds ratio to beta, keep std error
        >>> x3 = (None, 1.1, None, 1.30, 0.90, None, None) # convert odds ratio to beta, convert ci to standard error
        >>> x4 = (0.1, 1.1, None, 1.30, 0.90, None, None) # keep beta, convert ci to standard error
        >>> x5 = (None, 1.1, None, 1.30, 0.90, 9.0, -100) # convert beta to odds ratio, convert p-value and beta to standard error
        >>> x6 = (0.1, None, None, None, None, 9.0, -100) # keep beta, convert p-value and beta to standard error
        >>> x7 = (None, None, None, 1.3, 0.9, 9.0, -100) # keep beta NULL, without beta we do not want to compute the standard error
        >>> data = [x1, x2, x3, x4, x5, x6, x7]

        >>> schema = "beta FLOAT, oddsRatio FLOAT, standardError FLOAT, ci_upper FLOAT, ci_lower FLOAT, mantissa FLOAT, exp INT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +----+---------+-------------+--------+--------+--------+----+
        |beta|oddsRatio|standardError|ci_upper|ci_lower|mantissa| exp|
        +----+---------+-------------+--------+--------+--------+----+
        | 0.1|      1.1|          0.1|    NULL|    NULL|     9.0|-100|
        |NULL|      1.1|          0.1|    NULL|    NULL|     9.0|-100|
        |NULL|      1.1|         NULL|     1.3|     0.9|    NULL|NULL|
        | 0.1|      1.1|         NULL|     1.3|     0.9|    NULL|NULL|
        |NULL|      1.1|         NULL|     1.3|     0.9|     9.0|-100|
        | 0.1|     NULL|         NULL|    NULL|    NULL|     9.0|-100|
        |NULL|     NULL|         NULL|     1.3|     0.9|     9.0|-100|
        +----+---------+-------------+--------+--------+--------+----+
        <BLANKLINE>

        >>> beta = f.col("beta")
        >>> odds_ratio = f.col("oddsRatio")
        >>> se = f.col("standardError")
        >>> ci_upper = f.col("ci_upper")
        >>> ci_lower = f.col("ci_lower")
        >>> mantissa = f.col("mantissa")
        >>> exponent = f.col("exp")
        >>> cols = normalise_gwas_statistics(
        ...     beta, odds_ratio, se, ci_upper, ci_lower, mantissa, exponent
        ... )
        >>> beta_computed = f.round(cols.beta, 2).alias("beta")
        >>> standard_error_computed = f.round(cols.standard_error, 2).alias("standardError")
        >>> df.select(beta_computed, standard_error_computed).show()
        +----+-------------+
        |beta|standardError|
        +----+-------------+
        | 0.1|          0.1|
        | 0.1|          0.1|
        | 0.1|         0.09|
        | 0.1|         0.09|
        | 0.1|          0.0|
        | 0.1|          0.0|
        |NULL|         NULL|
        +----+-------------+
        <BLANKLINE>
    """
    beta = (
        f.when(beta.isNotNull(), beta)
        .when(odds_ratio.isNotNull(), f.log(odds_ratio))
        .otherwise(f.lit(None))
        .alias("beta")
    )
    chi2 = chi2_from_pvalue(mantissa, exponent)

    standard_error = (
        f.when(standard_error.isNotNull(), standard_error)
        .when(
            standard_error.isNull()
            & mantissa.isNotNull()
            & exponent.isNotNull()
            & beta.isNotNull(),
            stderr_from_chi2_and_effect_size(chi2, beta),
        )
        .when(
            standard_error.isNull()
            & ci_lower.isNotNull()
            & ci_upper.isNotNull()
            & odds_ratio.isNotNull(),
            stderr_from_ci(ci_upper, ci_lower),
        )
        .otherwise(f.lit(None))
        .alias("standardError")
    )

    return GWASEffect(standard_error=standard_error, beta=beta)


def pvalue_from_neglogpval(p_value: Column) -> PValComponents:
    """Computing p-value mantissa and exponent based on the negative 10 based logarithm of the p-value.

    Args:
        p_value (Column): Neg-log p-value (string)

    Returns:
        PValComponents: mantissa and exponent of the p-value

    Examples:
        >>> (
        ... spark.createDataFrame([(4.56, 'a'),(2109.23, 'b')], ['negLogPv', 'label'])
        ... .select('negLogPv',*pvalue_from_neglogpval(f.col('negLogPv')))
        ... .show()
        ... )
        +--------+--------------+--------------+
        |negLogPv|pValueMantissa|pValueExponent|
        +--------+--------------+--------------+
        |    4.56|     2.7542286|            -5|
        | 2109.23|     5.8884363|         -2110|
        +--------+--------------+--------------+
        <BLANKLINE>
    """
    exponent: Column = f.ceil(p_value)
    mantissa: Column = f.pow(f.lit(10), (exponent - p_value))

    return PValComponents(
        mantissa=mantissa.cast(t.FloatType()).alias("pValueMantissa"),
        exponent=(-1 * exponent).cast(t.IntegerType()).alias("pValueExponent"),
    )


def split_pvalue_column(pv: Column) -> PValComponents:
    """This function takes a p-value string and returns two columns mantissa (float), exponent (integer).

    Args:
        pv (Column): P-value as string

    Returns:
        PValComponents: pValueMantissa (float), pValueExponent (integer)

    Examples:
        >>> d = [("0.01",),("4.2E-45",),("43.2E5",),("0",),("1",)]
        >>> spark.createDataFrame(d, ['pval']).select('pval',*split_pvalue_column(f.col('pval'))).show()
        +-------+--------------+--------------+
        |   pval|pValueMantissa|pValueExponent|
        +-------+--------------+--------------+
        |   0.01|           1.0|            -2|
        |4.2E-45|           4.2|           -45|
        | 43.2E5|          43.2|             5|
        |      0|         2.225|          -308|
        |      1|           1.0|             0|
        +-------+--------------+--------------+
        <BLANKLINE>
    """
    # Making sure there's a number in the string:
    pv = f.when(
        pv == f.lit("0"), f.lit(sys.float_info.min).cast(t.StringType())
    ).otherwise(pv)

    # Get exponent:
    exponent = f.when(
        f.upper(pv).contains("E"),
        f.split(f.upper(pv), "E").getItem(1),
    ).otherwise(f.floor(f.log10(pv)))

    # Get mantissa:
    mantissa = f.when(
        f.upper(pv).contains("E"),
        f.split(f.upper(pv), "E").getItem(0),
    ).otherwise(pv / (10**exponent))

    # Round value:
    mantissa = f.round(mantissa, 3)

    return PValComponents(
        mantissa=mantissa.cast(t.FloatType()).alias("pValueMantissa"),
        exponent=exponent.cast(t.IntegerType()).alias("pValueExponent"),
    )


def stderr_from_chi2_and_effect_size(chi2_col: Column, beta: Column) -> Column:
    """Calculate standard error from chi2 and beta.

    This function calculates the standard error from the chi2 value and beta.

    Args:
        chi2_col (Column): Chi2 value (float)
        beta (Column): Beta value (float)

    Returns:
        Column: Standard error (float)

    Examples:
        >>> data = [(29.72, 3.0), (3.84, 1.0)]
        >>> schema = "chi2 FLOAT, beta FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +-----+----+
        | chi2|beta|
        +-----+----+
        |29.72| 3.0|
        | 3.84| 1.0|
        +-----+----+
        <BLANKLINE>

        >>> chi2_col = f.col("chi2")
        >>> beta = f.col("beta")
        >>> standard_error = f.round(stderr_from_chi2_and_effect_size(chi2_col, beta), 2).alias("standardError")
        >>> df2 = df.select(chi2_col, beta, standard_error)
        >>> df2.show()
        +-----+----+-------------+
        | chi2|beta|standardError|
        +-----+----+-------------+
        |29.72| 3.0|         0.55|
        | 3.84| 1.0|         0.51|
        +-----+----+-------------+
        <BLANKLINE>

    """
    return (f.abs(beta) / f.sqrt(chi2_col)).alias("standardError")


def stderr_from_ci(
    ci_upper: Column, ci_lower: Column, odds_ratio_based: bool = True
) -> Column:
    """Calculate standard error from confidence interval.

    This function calculates the standard error from the confidence interval upper and lower bounds.

    Args:
        ci_upper (Column): Upper bound of the confidence interval (float)
        ci_lower (Column): Lower bound of the confidence interval (float)
        odds_ratio_based (bool): If True, the function assumes that the confidence interval is based on odds ratio.
            use log difference (default), else it assumes that the confidence interval is based on beta.

    Returns:
        Column: Standard error (float)

    Note:
        Absolute value of the log difference is used to ensure that the standard error is always positive, even if the ci bounds are inverted.


    Examples:
        >>> data = [(0.5, 0.1), (1.0, 0.5)]
        >>> schema = "ci_upper FLOAT, ci_lower FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +--------+--------+
        |ci_upper|ci_lower|
        +--------+--------+
        |     0.5|     0.1|
        |     1.0|     0.5|
        +--------+--------+
        <BLANKLINE>

        >>> ci_upper = f.col("ci_upper")
        >>> ci_lower = f.col("ci_lower")
        >>> standard_error = f.round(stderr_from_ci(ci_upper, ci_lower), 2).alias("standardError")
        >>> df2 = df.select(ci_upper, ci_lower, standard_error)
        >>> df2.show()
        +--------+--------+-------------+
        |ci_upper|ci_lower|standardError|
        +--------+--------+-------------+
        |     0.5|     0.1|         0.41|
        |     1.0|     0.5|         0.18|
        +--------+--------+-------------+
        <BLANKLINE>
    """
    if odds_ratio_based:
        return (f.abs(f.log(ci_upper) - f.log(ci_lower)) / (2 * 1.96)).alias(
            "standardError"
        )
    return (f.abs(ci_upper - ci_lower) / (2 * 1.96)).alias("standardError")


def zscore_from_pvalue(pval_col: Column, beta: Column) -> Column:
    """Convert p-value column to z-score column.

    Args:
        pval_col (Column): p-value
        beta (Column): Effect size in beta - used to derive the sign of the z-score.

    Returns:
        Column: p-values transformed to z-scores

    Examples:
        >>> data = [("1.0", -1.0), ("0.9", -1.0), ("0.05", 1.0), ("1e-300", 1.0), ("1e-1000", None), (None, 1.0)]
        >>> schema = "pval STRING, beta FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +-------+----+
        |   pval|beta|
        +-------+----+
        |    1.0|-1.0|
        |    0.9|-1.0|
        |   0.05| 1.0|
        | 1e-300| 1.0|
        |1e-1000|NULL|
        |   NULL| 1.0|
        +-------+----+
        <BLANKLINE>

        >>> df.withColumn("zscore", zscore_from_pvalue(f.col("pval"), f.col("beta"))).show()
        +-------+----+--------------------+
        |   pval|beta|              zscore|
        +-------+----+--------------------+
        |    1.0|-1.0|                -0.0|
        |    0.9|-1.0|-0.12566134685507405|
        |   0.05| 1.0|   1.959963984540055|
        | 1e-300| 1.0|  37.065787880772135|
        |1e-1000|NULL|   67.75421020128564|
        |   NULL| 1.0|                NULL|
        +-------+----+--------------------+
        <BLANKLINE>

    """
    mantissa, exponent = split_pvalue_column(pval_col)
    sign = (
        f.when(beta > 0, f.lit(1))
        .when(beta < 0, f.lit(-1))
        .when(beta.isNull(), f.lit(1))
    )
    return (sign * f.sqrt(chi2_from_pvalue(mantissa, exponent))).alias("zscore")
