"""Trait for the association of one variant with a study's trait, as reported.

``Association`` holds the marginal statistics a study reports for a variant: the
effect size and its standard error, the p-value as a mantissa and exponent, the
effect allele frequency given by the source, and the sample size. Every column
is present wherever the trait is, but a source that does not report a statistic
leaves its values null.

The fields describe flat rows, one variant each. A dataset that nests them, such
as ``locus`` inside a study locus, has to flatten them before a method that asks
for ``Association`` can read them.

Example:
    >>> [field.name for field in Association.fields]  # doctest: +NORMALIZE_WHITESPACE
    ['variantId', 'beta', 'standardError', 'pValueMantissa', 'pValueExponent',
     'effectAlleleFrequencyFromSource', 'sampleSize']
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class Association(Trait):
    """Marginal statistics of one variant, as the study reported them."""

    variant_id = Field(t.StringType())
    beta = Field(t.DoubleType())
    standard_error = Field(t.DoubleType())
    p_value_mantissa = Field(t.FloatType())
    p_value_exponent = Field(t.IntegerType())
    effect_allele_frequency_from_source = Field(t.FloatType())
    sample_size = Field(t.IntegerType())
