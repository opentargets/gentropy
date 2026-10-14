"""Credible sets: the released fine-mapping results, at locus and at variant grain.

Fine-mapping a study locus gives zero or more credible sets. Each one is a set
of variants that together most likely contain the causal variant of one signal.
Two datasets hold them, and both are released, so neither refers to the staging
``StudyLocus``:

* ``CredibleSet``, one row per credible set: where it is, which study, its lead
  and the method that found it. Its key ``id`` is
  ``md5(studyId, chromosome, locusStart, locusEnd, leadVariantId,
  fineMappingMethod)``. The first four components rebuild the study locus key,
  so the parent locus can be recovered without storing it.
* ``CredibleSetVariant``, one row per variant of a credible set, keyed by
  ``credibleSetId`` and ``variantId``.

Columns copied from upstream are copies on purpose. ``chromosome``,
``locusStart``, ``locusEnd`` and ``studyId`` come from the study locus and
``studyType`` from the study index, so a consumer needs no other dataset. A
variant's marginal statistics come from the locus of the study locus; they
implement ``Association``, so they have the summary statistics' types and every
operation on associations, such as ``pvalue_filter``, applies.

``CredibleSet.qualityControls`` carries the study locus flags of the locus it
was fine-mapped from (``StudyLocusQualityFlag``), unlike the separate staging
and released flag sets of the PRD. ``CredibleSetVariant.qualityControls``
records how a variant's inputs were chosen, and why a variant has no marginal
statistics (``CredibleSetVariantQualityFlag``).

Every column of both datasets is mandatory, so the column is always present, but
a statistic can be null: a PICS credible set, for example, has no Bayes factor,
and its tag variants have no marginal p-value and no conditional effect.

Examples:
    >>> CredibleSet.print_schema()
    CredibleSet
     |-- id: string (mandatory: HasCredibleSetId.credible_set_id)
     |-- chromosome: string (mandatory: Region.chromosome)
     |-- locusStart: int (mandatory: Region.locus_start)
     |-- locusEnd: int (mandatory: Region.locus_end)
     |-- studyId: string (mandatory: HasStudyId.study_id)
     |-- studyType: string (mandatory: HasStudyType.study_type)
     |-- leadVariantId: string (mandatory: LeadVariant.variant_id, HasVariantId.variant_id)
     |-- fineMappingMethod: string (mandatory: HasFineMappingMethod.fine_mapping_method)
     |-- log10BayesFactor: double (mandatory)
     |-- confidence: tinyint (mandatory)
     |-- purityMeanR2: double (mandatory)
     |-- purityMinR2: double (mandatory)
     |-- qualityControls: array<string> (mandatory: QualityControls.quality_controls)
    >>> CredibleSetVariant.print_schema()
    CredibleSetVariant
     |-- credibleSetId: string (mandatory: HasCredibleSetId.credible_set_id)
     |-- variantId: string (mandatory: HasVariantId.variant_id, Association.variant_id)
     |-- pValueMantissa: float (mandatory: Association.p_value_mantissa)
     |-- pValueExponent: int (mandatory: Association.p_value_exponent)
     |-- effectAlleleFrequency: float (mandatory: Association.effect_allele_frequency_from_source)
     |-- sampleSize: int (mandatory: Association.sample_size)
     |-- reportedBeta: double (mandatory: Association.beta)
     |-- reportedStandardError: double (mandatory: Association.standard_error)
     |-- posteriorProbability: float (mandatory: PosteriorProbability.posterior_probability)
     |-- log10BayesFactor: float (mandatory)
     |-- r2Overall: float (mandatory)
     |-- conditionalBeta: float (mandatory)
     |-- conditionalStandardError: float (mandatory)
     |-- rescaledAbsBeta: float (mandatory)
     |-- rescaledStandardError: float (mandatory)
     |-- rescaledDirection: tinyint (mandatory)
     |-- qualityControls: array<string> (mandatory: QualityControls.quality_controls)

    A PICS credible set of two variants, its lead and one tag. The key is built
    from ``ID_COLUMNS``:

    >>> import hashlib
    >>> credible_sets = CredibleSet(
    ...     spark.createDataFrame(
    ...         [
    ...             ("", "1", 100_000, 600_000, "GCST1", "gwas", "1_350000_A_T", "PICS",
    ...              None, None, None, None, ["Variant not found in LD reference"]),
    ...         ],
    ...         CredibleSet.schema,
    ...     ).withColumn("id", CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS))
    ... )
    >>> CredibleSet.ID_COLUMNS
    ('studyId', 'chromosome', 'locusStart', 'locusEnd', 'leadVariantId', 'fineMappingMethod')
    >>> key = credible_sets.df.select(credible_sets.id).first()[0]
    >>> key == hashlib.md5(b"GCST111000006000001_350000_A_TPICS").hexdigest()
    True

    The lead carries its marginal statistics; the tag only its posterior
    probability and its LD with the lead:

    >>> variants = CredibleSetVariant(
    ...     spark.createDataFrame(
    ...         [
    ...             (key, "1_350000_A_T", 1.0, -10, 0.3, 5000, 0.2, 0.01, 0.7,
    ...              None, 1.0, None, None, None, None, None, []),
    ...             (key, "1_350100_C_G", None, None, None, None, None, None, 0.3,
    ...              None, 0.8, None, None, None, None, None, []),
    ...         ],
    ...         CredibleSetVariant.schema,
    ...     )
    ... )
    >>> variants.df.select(
    ...     variants.variant_id, variants.p_value_exponent, variants.posterior_probability
    ... ).show()
    +------------+--------------+--------------------+
    |   variantId|pValueExponent|posteriorProbability|
    +------------+--------------+--------------------+
    |1_350000_A_T|           -10|                 0.7|
    |1_350100_C_G|          NULL|                 0.3|
    +------------+--------------+--------------------+
    <BLANKLINE>

    Both datasets hold the credible set key, under their own column names:

    >>> HasCredibleSetId.of(credible_sets).credible_set_id
    Column<'id'>
    >>> HasCredibleSetId.of(variants).credible_set_id
    Column<'credibleSetId'>

    Variant flags are written to ``qualityControls`` by value:

    >>> [flag.name for flag in CredibleSetVariantQualityFlag]
    ['EAF_FROM_SOURCE', 'EAF_FROM_GNOMAD', 'RESCALED_FROM_CONDITIONAL', 'RESCALED_FROM_MARGINAL', 'NOT_IN_LOCUS']
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar

from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements, required
from gentropy.model.trait.association import Association
from gentropy.model.trait.fine_mapping import HasFineMappingMethod, PosteriorProbability
from gentropy.model.trait.identifier import HasCredibleSetId, HasStudyId, HasVariantId
from gentropy.model.trait.locus import LeadVariant, Region
from gentropy.model.trait.quality_control import QualityControls
from gentropy.model.trait.study import HasStudyType


class CredibleSetVariantQualityFlag(Enum):
    """How the inputs of a credible set variant were chosen.

    Attributes:
        EAF_FROM_SOURCE (str): Allele frequency reported by the study
        EAF_FROM_GNOMAD (str): Allele frequency taken from gnomAD, as the study reported none
        RESCALED_FROM_CONDITIONAL (str): Rescaled effect derived from the conditional effect
        RESCALED_FROM_MARGINAL (str): Rescaled effect derived from the marginal effect
        NOT_IN_LOCUS (str): Variant in LD with the lead has no statistics in the
            locus, so its marginal statistics are null
    """

    EAF_FROM_SOURCE = "Allele frequency reported by the study"
    EAF_FROM_GNOMAD = "Allele frequency taken from gnomAD"
    RESCALED_FROM_CONDITIONAL = "Rescaled effect derived from the conditional effect"
    RESCALED_FROM_MARGINAL = "Rescaled effect derived from the marginal effect"
    NOT_IN_LOCUS = "Variant in LD with the lead has no statistics in the locus"


class CredibleSet(
    Dataset[
        HasCredibleSetId
        | Region
        | HasStudyId
        | HasStudyType
        | LeadVariant
        | HasVariantId
        | HasFineMappingMethod
        | QualityControls
    ]
):
    """One fine-mapping result: a credible set of one study, at locus grain."""

    ID_COLUMNS: ClassVar[tuple[str, ...]]
    """Columns whose values define the key ``id``, hashed by ``generate_identifier``."""

    id = implements(HasCredibleSetId.credible_set_id)
    chromosome = implements(Region.chromosome)
    locus_start = implements(Region.locus_start)
    locus_end = implements(Region.locus_end)
    study_id = implements(HasStudyId.study_id)
    study_type = implements(HasStudyType.study_type)
    lead_variant_id = implements(LeadVariant.variant_id, HasVariantId.variant_id)
    fine_mapping_method = implements(HasFineMappingMethod.fine_mapping_method)
    log10_bayes_factor = required(t.DoubleType())
    confidence = required(t.ByteType())
    purity_mean_r2 = required(t.DoubleType())
    purity_min_r2 = required(t.DoubleType())
    quality_controls = implements(QualityControls.quality_controls)


# Column names exist only once the class is created, and ``leadVariantId`` is not
# its trait field's name (``variantId``), so the key is listed here.
CredibleSet.ID_COLUMNS = (
    CredibleSet.study_id.name,
    CredibleSet.chromosome.name,
    CredibleSet.locus_start.name,
    CredibleSet.locus_end.name,
    CredibleSet.lead_variant_id.name,
    CredibleSet.fine_mapping_method.name,
)


class CredibleSetVariant(
    Dataset[
        HasCredibleSetId
        | HasVariantId
        | Association
        | PosteriorProbability
        | QualityControls
    ]
):
    """One variant of a credible set, with its marginal and fine-mapping statistics."""

    credible_set_id = implements(HasCredibleSetId.credible_set_id)
    variant_id = implements(HasVariantId.variant_id, Association.variant_id)
    p_value_mantissa = implements(Association.p_value_mantissa)
    p_value_exponent = implements(Association.p_value_exponent)
    effect_allele_frequency = implements(
        Association.effect_allele_frequency_from_source
    )
    sample_size = implements(Association.sample_size)
    reported_beta = implements(Association.beta)
    reported_standard_error = implements(Association.standard_error)
    posterior_probability = implements(PosteriorProbability.posterior_probability)
    log10_bayes_factor = required(t.FloatType())
    r2_overall = required(t.FloatType())
    conditional_beta = required(t.FloatType())
    conditional_standard_error = required(t.FloatType())
    rescaled_abs_beta = required(t.FloatType())
    rescaled_standard_error = required(t.FloatType())
    rescaled_direction = required(t.ByteType())
    quality_controls = implements(QualityControls.quality_controls)
