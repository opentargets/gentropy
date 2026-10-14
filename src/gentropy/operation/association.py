"""Operations on datasets with single-variant associations."""

from __future__ import annotations

from typing import TypeVar

from gentropy.common.stats import split_pvalue
from gentropy.model.core.dataset import Dataset
from gentropy.model.trait.association import Association

D = TypeVar("D", bound=Dataset[Association])


def pvalue_filter(dataset: D, pvalue: float) -> D:
    """Keep the rows at least as significant as ``pvalue``.

    The threshold is split into a mantissa and an exponent, like the p-value
    columns. A smaller exponent is always more significant, and the mantissa
    decides only between equal exponents. A row whose p-value equals the
    threshold is kept.

    Args:
        dataset (D): Any dataset with the ``Association`` trait.
        pvalue (float): The largest p-value to keep, between 0 and 1.

    Returns:
        D: The rows that pass, as the same dataset type.

    Examples:
        >>> from gentropy.dataset.summary_statistics import SummaryStatistics
        >>> rows = [
        ...     ("GCST1", "1_100_A_T", "1", 100, 0.1, 5000, 5.0, -8, 0.3, 0.01),
        ...     ("GCST1", "1_200_C_G", "1", 200, 0.1, 5000, 5.1, -8, 0.3, 0.01),
        ... ]
        >>> sumstats = SummaryStatistics(spark.createDataFrame(rows, SummaryStatistics.schema))
        >>> significant = sumstats.pipe(pvalue_filter, 5e-8)
        >>> significant.df.select(significant.variant_id).show()
        +---------+
        |variantId|
        +---------+
        |1_100_A_T|
        +---------+
        <BLANKLINE>
    """
    mantissa, exponent = split_pvalue(pvalue)
    association = Association.of(dataset)
    return type(dataset)(
        dataset.df.filter(
            (association.p_value_exponent < exponent)
            | (
                (association.p_value_exponent == exponent)
                & (association.p_value_mantissa <= mantissa)
            )
        )
    )
