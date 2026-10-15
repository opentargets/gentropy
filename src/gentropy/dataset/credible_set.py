"""Credible sets: the fine-mapping results, at locus and at variant grain.

Fine-mapping a study locus gives zero or more credible sets. Each one is a set of variants
that together most likely contain the causal variant of one signal. Two datasets hold them:

- `CredibleSet`, one row per credible set, keyed by `id`.
- `CredibleSetVariant`, one row per variant of a credible set, keyed by `credibleSetId` and `variantId`.

The credible set key hashes `CredibleSet.ID_COLUMNS`. Its first four columns rebuild the study
locus key, so the parent study locus can be recovered without storing its identifier.

Keys, the lead, the method, the posterior probability and the quality controls are never null.
Every statistic is nullable, as not every method or source gives it: a PICS credible set, for
example, has no Bayes factor, and its tag variants have no conditional effect.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus import StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


class CredibleSetVariantQualityCheck(Enum):
    """How the inputs of a credible set variant were chosen.

    Attributes:
        EAF_FROM_SOURCE (str): Allele frequency reported by the study
        EAF_FROM_GNOMAD (str): Allele frequency taken from gnomAD, as the study reported none
        RESCALED_FROM_CONDITIONAL (str): Rescaled effect derived from the conditional effect
        RESCALED_FROM_MARGINAL (str): Rescaled effect derived from the marginal effect
        NOT_IN_LOCUS (str): Variant has no statistics in the locus, so its marginal statistics are null
    """

    EAF_FROM_SOURCE = "Allele frequency reported by the study"
    EAF_FROM_GNOMAD = "Allele frequency taken from gnomAD"
    RESCALED_FROM_CONDITIONAL = "Rescaled effect derived from the conditional effect"
    RESCALED_FROM_MARGINAL = "Rescaled effect derived from the marginal effect"
    NOT_IN_LOCUS = "Variant in LD with the lead has no statistics in the locus"


@dataclass
class CredibleSet(Dataset):
    """One fine-mapping result: a credible set of one study, at locus grain.

    `chromosome`, `locusStart`, `locusEnd` and `studyId` are copied from the study locus and
    `studyType` from the study index, so a consumer needs no other dataset. `qualityControls`
    carries the `StudyLocusQualityCheck` flags of the study locus it was fine-mapped from.

    Examples:
        >>> import hashlib
        >>> rows = [
        ...     ("", "1", 100_000, 600_000, "GCST1", "gwas", "1_350000_A_T", "PICS",
        ...      None, None, None, None, ["Variant not found in LD reference"]),
        ... ]
        >>> credible_sets = CredibleSet(
        ...     spark.createDataFrame(rows, CredibleSet.get_schema())
        ...     .withColumn("id", CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)),
        ...     CredibleSet.get_schema(),
        ... )
        >>> CredibleSet.ID_COLUMNS
        ('studyId', 'chromosome', 'locusStart', 'locusEnd', 'leadVariantId', 'fineMappingMethod')
        >>> credible_sets.df.first().id == hashlib.md5(
        ...     b'{"studyId":"GCST1","chromosome":"1","locusStart":100000,"locusEnd":600000,'
        ...     b'"leadVariantId":"1_350000_A_T","fineMappingMethod":"PICS"}'
        ... ).hexdigest()
        True
    """

    ID_COLUMNS: ClassVar[tuple[str, ...]] = (
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
        "leadVariantId",
        "fineMappingMethod",
    )
    """Columns whose values define the key `id`, hashed by `generate_identifier`."""

    @classmethod
    def get_schema(cls: type[CredibleSet]) -> StructType:
        """Provides the schema for the CredibleSet dataset.

        Returns:
            StructType: schema for the CredibleSet dataset.
        """
        return parse_spark_schema("credible_set.json")

    @classmethod
    def get_QC_column_name(cls: type[CredibleSet]) -> str:
        """Quality control column.

        Returns:
            str: Name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[CredibleSet]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in StudyLocusQualityCheck}


@dataclass
class CredibleSetVariant(Dataset):
    """One variant of a credible set, with its marginal and fine-mapping statistics.

    The marginal statistics (`pValueMantissa`, `pValueExponent`, `effectAlleleFrequency`,
    `sampleSize`, `reportedBeta`, `reportedStandardError`) are the ones the study reported. The
    conditional effect of the fine-mapping method, when it gives one, is kept apart in
    `conditionalBeta` and `conditionalStandardError`. Bayes factors are base-10 logarithms.

    Examples:
        >>> rows = [
        ...     ("cs1", "1_350000_A_T", 1.0, -10, 0.3, 5000, 0.2, 0.01, 0.7,
        ...      None, 1.0, None, None, None, None, None, []),
        ...     ("cs1", "1_350100_C_G", None, None, None, None, None, None, 0.3,
        ...      None, 0.8, None, None, None, None, None,
        ...      ["Variant in LD with the lead has no statistics in the locus"]),
        ... ]
        >>> variants = CredibleSetVariant(
        ...     spark.createDataFrame(rows, CredibleSetVariant.get_schema()),
        ...     CredibleSetVariant.get_schema(),
        ... )
        >>> variants.df.select("variantId", "pValueExponent", "posteriorProbability").show()
        +------------+--------------+--------------------+
        |   variantId|pValueExponent|posteriorProbability|
        +------------+--------------+--------------------+
        |1_350000_A_T|           -10|                 0.7|
        |1_350100_C_G|          NULL|                 0.3|
        +------------+--------------+--------------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[CredibleSetVariant]) -> StructType:
        """Provides the schema for the CredibleSetVariant dataset.

        Returns:
            StructType: schema for the CredibleSetVariant dataset.
        """
        return parse_spark_schema("credible_set_variant.json")

    @classmethod
    def get_QC_column_name(cls: type[CredibleSetVariant]) -> str:
        """Quality control column.

        Returns:
            str: Name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[CredibleSetVariant]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in CredibleSetVariantQualityCheck}
