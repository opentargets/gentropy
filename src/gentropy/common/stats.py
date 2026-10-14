"""Statistics helpers."""

from __future__ import annotations

from math import floor, log10

from pyspark.sql import Column
from pyspark.sql import functions as f


def split_pvalue(pvalue: float) -> tuple[float, int]:
    """Split a p-value into its base-10 mantissa and exponent.

    Datasets store p-values as ``pValueMantissa`` and ``pValueExponent`` so that
    values below the smallest double stay representable. This turns a threshold
    into the same form, so it can be compared with those columns. The mantissa
    is rounded to three decimals, and a p-value of 0 splits into ``(0.0, 0)``.

    Args:
        pvalue (float): The p-value, between 0 and 1.

    Returns:
        tuple[float, int]: The mantissa and the exponent.

    Raises:
        ValueError: If the p-value is not between 0 and 1.

    Examples:
        >>> split_pvalue(0.00001234)
        (1.234, -5)
        >>> split_pvalue(5e-8)
        (5.0, -8)
        >>> split_pvalue(1)
        (1.0, 0)
        >>> split_pvalue(0.99)
        (9.9, -1)
    """
    if pvalue < 0.0 or pvalue > 1.0:
        raise ValueError("P-value must be between 0 and 1")

    exponent = floor(log10(pvalue)) if pvalue != 0 else 0
    mantissa = round(pvalue / 10**exponent, 3)
    return (mantissa, exponent)


def neglog_pvalue(mantissa: Column, exponent: Column) -> Column:
    """The ``-log10`` of a p-value stored as a mantissa and an exponent.

    It is computed from the two parts, so p-values below the smallest double
    keep their exact strength. A mantissa of 0 has no logarithm and gives null.

    Args:
        mantissa (Column): The p-value's base-10 mantissa.
        exponent (Column): The p-value's base-10 exponent.

    Returns:
        Column: The p-value's ``-log10``, as a double.

    Examples:
        >>> df = spark.createDataFrame([(1.0, -10), (5.0, -8), (2.5, -400)], "m float, e int")
        >>> [round(row.p, 3) for row in df.select(neglog_pvalue(df.m, df.e).alias("p")).collect()]
        [10.0, 7.301, 399.602]
    """
    return -(f.log10(mantissa) + exponent)
