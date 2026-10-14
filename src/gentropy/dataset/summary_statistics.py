"""Summary statistics: the single-variant results of one study, one row per variant.

Each row is the association of one variant with the study's trait, as the study
reported it. ``studyId`` is the key into the study index and partitions the
summary statistics themselves; ``variantId`` is the key into the variant index.

Every association statistic is mandatory, so the column is always present, but a
source that does not report a statistic leaves its values null. ``chromosome``
and ``position`` locate the variant, which window clumping needs.

Examples:
    >>> SummaryStatistics.print_schema()
    SummaryStatistics
     |-- studyId: string (mandatory: HasStudyId.study_id)
     |-- variantId: string (mandatory: HasVariantId.variant_id, Association.variant_id)
     |-- chromosome: string (mandatory: VariantLocation.chromosome)
     |-- position: int (mandatory: VariantLocation.position)
     |-- beta: double (mandatory: Association.beta)
     |-- sampleSize: int (mandatory: Association.sample_size)
     |-- pValueMantissa: float (mandatory: Association.p_value_mantissa)
     |-- pValueExponent: int (mandatory: Association.p_value_exponent)
     |-- effectAlleleFrequencyFromSource: float (mandatory: Association.effect_allele_frequency_from_source)
     |-- standardError: double (mandatory: Association.standard_error)

    One association from a source that reports no standard error:

    >>> rows = [("GCST1", "1_100_A_T", "1", 100, 0.21, 5000, 1.5, -8, 0.3, None)]
    >>> sumstats = SummaryStatistics(spark.createDataFrame(rows, SummaryStatistics.schema))
    >>> sumstats.df.select(
    ...     sumstats.variant_id, sumstats.p_value_exponent, sumstats.standard_error
    ... ).show()
    +---------+--------------+-------------+
    |variantId|pValueExponent|standardError|
    +---------+--------------+-------------+
    |1_100_A_T|            -8|         NULL|
    +---------+--------------+-------------+
    <BLANKLINE>
"""

from __future__ import annotations

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements
from gentropy.model.trait.association import Association
from gentropy.model.trait.identifier import (
    HasStudyId,
    HasVariantId,
)
from gentropy.model.trait.locus import VariantLocation


class SummaryStatistics(
    Dataset[HasStudyId | HasVariantId | Association | VariantLocation]
):
    """Single-variant association results of one study."""

    study_id = implements(HasStudyId.study_id)
    variant_id = implements(HasVariantId.variant_id, Association.variant_id)
    chromosome = implements(VariantLocation.chromosome)
    position = implements(VariantLocation.position)
    beta = implements(Association.beta)
    sample_size = implements(Association.sample_size)
    p_value_mantissa = implements(Association.p_value_mantissa)
    p_value_exponent = implements(Association.p_value_exponent)
    effect_allele_frequency_from_source = implements(
        Association.effect_allele_frequency_from_source
    )
    standard_error = implements(Association.standard_error)
