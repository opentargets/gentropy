"""Study locus dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, Self

import pyspark.sql.functions as f

from gentropy.common.genomic_region import GenomicRegion
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.stats import split_pvalue
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


class StudyLocusQualityCheck(Enum):
    """Study locus quality control flags, raised by curation, clumping, LD annotation and fine-mapping.

    `CredibleSet.qualityControls` carries the flags of the study locus it was fine-mapped from.

    Attributes:
        WINDOW_CLUMPED (str): Explained by a more significant variant in the same window
        LD_CLUMPED (str): Explained by a more significant variant in high LD
        NO_POPULATION (str): Study does not have population annotation to resolve LD
        UNRESOLVED_LD (str): Variant not found in LD reference
        OUT_OF_SAMPLE_LD (str): Study locus finemapped without in-sample LD reference
        SUBSIGNIFICANT_FLAG (str): Curated top hit with a p-value above the significance threshold
        COMPOSITE_FLAG (str): Curated top hit of several variants, such as an interaction or a haplotype
        INCONSISTENCY_FLAG (str): Curated top hit whose location or variants disagree
        PALINDROMIC_ALLELE_FLAG (str): Curated top hit with palindromic alleles, whose effect cannot be harmonised
        TOP_HIT (str): Study locus from curated top hit
        TRANS_QTL (str): QTL credible set acting in trans on its measured gene, raised by the platform
            transformation stage
        REPLICATED (str): Lead variant is replicated in an independent study, raised by the platform
            transformation stage
    """

    WINDOW_CLUMPED = "Explained by a more significant variant in the same window"
    LD_CLUMPED = "Explained by a more significant variant in high LD"
    NO_POPULATION = "Study does not have population annotation to resolve LD"
    UNRESOLVED_LD = "Variant not found in LD reference"
    OUT_OF_SAMPLE_LD = "Study locus finemapped without in-sample LD reference"
    SUBSIGNIFICANT_FLAG = "Subsignificant p-value"
    COMPOSITE_FLAG = "Composite association"
    INCONSISTENCY_FLAG = "Variant inconsistency"
    PALINDROMIC_ALLELE_FLAG = "Palindrome alleles - cannot harmonize"
    TOP_HIT = "Study locus from curated top hit"
    TRANS_QTL = "Trans QTL"
    REPLICATED = "Lead variant is replicated in an independent study"


class FinemappingMethod(Enum):
    """Finemapping method enum.

    Attributes:
        PICS (str): PICS
        SUSIE (str): SuSiE method
        SUSIE_INF (str): SuSiE-inf method implemented in `gentropy`
    """

    PICS = "PICS"
    SUSIE = "SuSiE"
    SUSIE_INF = "SuSiE-inf"

    @classmethod
    def methods_with_lbf(cls) -> list[str]:
        """List of finemapping methods that report log Bayes factors.

        Returns:
            list[str]: List of finemapping methods that report log Bayes factors.
        """
        return [cls.SUSIE.value, cls.SUSIE_INF.value]


@dataclass
class StudyLocus(Dataset):
    """Study locus: a region of one study that clumping found worth fine-mapping.

    Produced by window, locus-breaker and LD clumping and consumed by fine-mapping. Its key
    `id` hashes `ID_COLUMNS`, and its lead `leadVariantId` is the variant clumping used to
    define the locus. `ldSet` holds the variants correlated with the lead, once the locus
    went through LD annotation.

    A study locus comes in three states, each a subclass of the previous one:

    - `StudyLocus`, as clumping produces it.
    - `AnnotatedStudyLocus`, which adds `locus`: the marginal statistics of every variant the study measured inside the region.
    - `LDAnnotatedStudyLocus`, whose `ldSet` is mandatory.

    Examples:
        >>> import hashlib
        >>> ld_set = [("1_100501_G_A", 0.5)]
        >>> rows = [
        ...     ("", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], ld_set),
        ...     ("", "GCST1", "2", 100_000, 500_000, "2_100500_C_T", [], None),
        ... ]
        >>> loci = StudyLocus(
        ...     spark.createDataFrame(rows, StudyLocus.get_schema())
        ...     .withColumn("id", StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS)),
        ...     StudyLocus.get_schema(),
        ... )
        >>> loci.df.select("chromosome", "leadVariantId", "ldSet").show(truncate=False)
        +----------+-------------+---------------------+
        |chromosome|leadVariantId|ldSet                |
        +----------+-------------+---------------------+
        |1         |1_100500_C_T |[{1_100501_G_A, 0.5}]|
        |2         |2_100500_C_T |NULL                 |
        +----------+-------------+---------------------+
        <BLANKLINE>
        >>> StudyLocus.ID_COLUMNS
        ('studyId', 'chromosome', 'locusStart', 'locusEnd')
        >>> loci.df.filter("chromosome = '1'").first().id == hashlib.md5(
        ...     b'{"studyId":"GCST1","chromosome":"1","locusStart":100000,"locusEnd":500000}'
        ... ).hexdigest()
        True
    """

    ID_COLUMNS: ClassVar[tuple[str, ...]] = (
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
    )
    """Columns whose values define the key `id`, hashed by `generate_identifier`."""

    @classmethod
    def get_schema(cls: type[StudyLocus]) -> StructType:
        """Provides the schema for the StudyLocus dataset.

        Returns:
            StructType: schema for the StudyLocus dataset.
        """
        return parse_spark_schema("study_locus.json")

    @classmethod
    def get_QC_column_name(cls: type[StudyLocus]) -> str:
        """Quality control column.

        Returns:
            str: Name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[StudyLocus]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in StudyLocusQualityCheck}

    def exclude_region(
        self: StudyLocus, region: GenomicRegion, exclude_overlap: bool = False
    ) -> StudyLocus:
        """Exclude a region from the StudyLocus dataset.

        Args:
            region (GenomicRegion): genomic region object.
            exclude_overlap (bool): If True, excludes StudyLocus windows with any overlap with the region.

        Returns:
            StudyLocus: filtered StudyLocus object.
        """
        if exclude_overlap:
            filter_condition = ~(
                (f.col("chromosome") == region.chromosome)
                & (
                    (f.col("locusStart") <= region.end)
                    & (f.col("locusEnd") >= region.start)
                )
            )
        else:
            filter_condition = ~(
                (f.col("chromosome") == region.chromosome)
                & (
                    (f.col("position") >= region.start)
                    & (f.col("position") <= region.end)
                )
            )

        return StudyLocus(
            _df=self.df.filter(filter_condition),
            _schema=StudyLocus.get_schema(),
        )


@dataclass
class AnnotatedStudyLocus(StudyLocus):
    """Study locus whose `locus` holds the marginal statistics of every variant inside its region.

    `locus` is collected from the summary statistics of the same study and chromosome, between
    `locusStart` and `locusEnd` (both included), sorted by position. It is empty, not null,
    when the study measured no variant inside the region.

    Examples:
        >>> locus = [("1_100500_C_T", 1.0, -10, 0.3, 5000, 0.2, 0.01)]
        >>> rows = [("sl1", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], locus, None)]
        >>> loci = AnnotatedStudyLocus(
        ...     spark.createDataFrame(rows, AnnotatedStudyLocus.get_schema()),
        ...     AnnotatedStudyLocus.get_schema(),
        ... )
        >>> loci.df.select("id", f.explode("locus.beta").alias("beta")).show()
        +---+----+
        | id|beta|
        +---+----+
        |sl1| 0.2|
        +---+----+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[AnnotatedStudyLocus]) -> StructType:
        """Provides the schema for the AnnotatedStudyLocus dataset.

        Returns:
            StructType: schema for the AnnotatedStudyLocus dataset.
        """
        return parse_spark_schema("annotated_study_locus.json")

    def lead_pvalue_filter(self: Self, pvalue: float) -> Self:
        """Keep the loci whose lead is at least as significant as `pvalue`.

        The lead's p-value is its own entry in `locus`. A locus whose lead has no
        entry, or no full p-value, is dropped.

        Args:
            pvalue (float): The largest lead p-value to keep, between 0 and 1.

        Returns:
            Self: The loci that pass, as the same dataset type.

        Examples:
            >>> def locus(lead, exponent):
            ...     return [(lead, 5.0, exponent, 0.3, 1000, 0.1, 0.01)]
            >>> rows = [
            ...     ("L1", "S1", "1", 1, 1000, "A", [], locus("A", -8), None),
            ...     ("L2", "S1", "1", 1, 1000, "B", [], locus("B", -7), None),
            ...     ("L3", "S1", "1", 1, 1000, "C", [], [], None),
            ... ]
            >>> loci = AnnotatedStudyLocus(
            ...     spark.createDataFrame(rows, AnnotatedStudyLocus.get_schema()),
            ...     AnnotatedStudyLocus.get_schema(),
            ... )
            >>> [row.id for row in loci.lead_pvalue_filter(5e-8).df.collect()]
            ['L1']
        """
        mantissa, exponent = split_pvalue(pvalue)
        lead = f.get(
            f.filter("locus", lambda e: e["variantId"] == f.col("leadVariantId")), 0
        )
        return self.filter(
            (lead["pValueExponent"] < exponent)
            | (
                (lead["pValueExponent"] == exponent)
                & (lead["pValueMantissa"] <= mantissa)
            )
        )


@dataclass
class LDAnnotatedStudyLocus(AnnotatedStudyLocus):
    """Annotated study locus whose `ldSet` holds the variants in LD with the lead.

    `r2Overall` is the r² with the lead in the major population of the study. The lead is
    always part of its own `ldSet`, with an r² of 1.

    Examples:
        >>> ld_set = [("1_100500_C_T", 1.0), ("1_100501_G_A", 0.5)]
        >>> rows = [("sl1", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], [], ld_set)]
        >>> loci = LDAnnotatedStudyLocus(
        ...     spark.createDataFrame(rows, LDAnnotatedStudyLocus.get_schema()),
        ...     LDAnnotatedStudyLocus.get_schema(),
        ... )
        >>> loci.df.select("id", f.size("ldSet").alias("tags")).show()
        +---+----+
        | id|tags|
        +---+----+
        |sl1|   2|
        +---+----+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[LDAnnotatedStudyLocus]) -> StructType:
        """Provides the schema for the LDAnnotatedStudyLocus dataset.

        Returns:
            StructType: schema for the LDAnnotatedStudyLocus dataset.
        """
        return parse_spark_schema("ld_annotated_study_locus.json")
