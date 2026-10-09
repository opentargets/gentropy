"""Study locus: the staging result of clumping, one row per locus.

A study locus is a region of one study that clumping found worth fine-mapping. It is
produced by locus-breaker, window and LD clumping, consumed by fine-mapping, and
never released. Its key ``id`` is ``md5(studyId, chromosome, locusStart, locusEnd)``.

Its lead variant is the variant clumping used to define the locus. ``studyId``
serves both joins that need the study, to the study index and to the summary
statistics, and ``leadVariantId`` is both the lead and the key into the variant
index. ``ldSet`` and ``locus`` are optional and hold the variants around the
lead: those correlated with it, and those the study measured. Their fields sit
inside arrays, so no trait describes them; code that needs them per variant
flattens them first.

Other datasets refer to a study locus through the ``CanJoinToStudyLocus`` trait.
They store its field under their own column name; ``StudyLocus`` itself stores it
as ``id``. Code reads the key as ``CanJoinToStudyLocus.of(ds).study_locus_id``,
which resolves to ``id`` here and to the other dataset's column elsewhere. Code
written for ``StudyLocus`` itself reads columns as attributes, such as
``loci.lead_variant_id``.

Examples:
    >>> StudyLocus.print_schema()
    StudyLocus
     |-- id: string (mandatory: CanJoinToStudyLocus.study_locus_id)
     |-- studyId: string (mandatory: CanJoinToStudyIndex.study_id, CanJoinToSummaryStatistics.study_id)
     |-- chromosome: string (mandatory: Region.chromosome)
     |-- locusStart: int (mandatory: Region.locus_start)
     |-- locusEnd: int (mandatory: Region.locus_end)
     |-- leadVariantId: string (mandatory: LeadVariant.variant_id, CanJoinToVariantIndex.variant_id)
     |-- ldSet: array (optional)
     |    |-- element: struct
     |    |    |-- tagVariantId: string
     |    |    |-- r2Overall: float
     |-- locus: array (optional)
     |    |-- element: struct
     |    |    |-- variantId: string
     |    |    |-- pValueMantissa: double
     |    |    |-- pValueExponent: int
     |    |    |-- effectAlleleFrequencyFromSource: float
     |    |    |-- sampleSize: int
     |    |    |-- beta: double
     |    |    |-- standardError: double
     |-- qualityControls: array<string> (mandatory: QualityControls.quality_controls)

    One locus annotated with LD and one with summary statistics:

    >>> ld_set = [("1_100501_G_A", 0.5)]
    >>> locus = [("2_100501_A_T", 1.95, -11, 0.5, 5000, 0.21, 0.01)]
    >>> rows = [
    ...     ("0feec5fa", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", ld_set, None, []),
    ...     ("991ffbcc", "GCST1", "2", 100_000, 500_000, "2_100500_C_T", None, locus, []),
    ... ]
    >>> loci = StudyLocus(spark.createDataFrame(rows, StudyLocus.schema))
    >>> loci.df.select(
    ...     CanJoinToStudyLocus.of(loci).study_locus_id,
    ...     loci.lead_variant_id,
    ...     loci.ld_set,
    ...     loci.locus,
    ... ).show(truncate=False)
    +--------+-------------+---------------------+--------------------------------------------------+
    |id      |leadVariantId|ldSet                |locus                                             |
    +--------+-------------+---------------------+--------------------------------------------------+
    |0feec5fa|1_100500_C_T |[{1_100501_G_A, 0.5}]|NULL                                              |
    |991ffbcc|2_100500_C_T |NULL                 |[{2_100501_A_T, 1.95, -11, 0.5, 5000, 0.21, 0.01}]|
    +--------+-------------+---------------------+--------------------------------------------------+
    <BLANKLINE>
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements, optional
from gentropy.model.trait.join import (
    CanJoinToStudyIndex,
    CanJoinToStudyLocus,
    CanJoinToSummaryStatistics,
    CanJoinToVariantIndex,
)
from gentropy.model.trait.locus import LeadVariant, Region
from gentropy.model.trait.quality_control import QualityControls


class StudyLocus(
    Dataset[
        CanJoinToStudyLocus
        | CanJoinToStudyIndex
        | CanJoinToSummaryStatistics
        | CanJoinToVariantIndex
        | Region
        | LeadVariant
        | QualityControls
    ]
):
    """Clumped region of one study, staged for fine-mapping."""

    id = implements(CanJoinToStudyLocus.study_locus_id)
    study_id = implements(
        CanJoinToStudyIndex.study_id, CanJoinToSummaryStatistics.study_id
    )
    chromosome = implements(Region.chromosome)
    locus_start = implements(Region.locus_start)
    locus_end = implements(Region.locus_end)
    lead_variant_id = implements(
        LeadVariant.variant_id, CanJoinToVariantIndex.variant_id
    )
    ld_set = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("tagVariantId", t.StringType()),
                    t.StructField("r2Overall", t.FloatType()),
                ]
            )
        )
    )
    locus = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("variantId", t.StringType()),
                    t.StructField("pValueMantissa", t.DoubleType()),
                    t.StructField("pValueExponent", t.IntegerType()),
                    t.StructField("effectAlleleFrequencyFromSource", t.FloatType()),
                    t.StructField("sampleSize", t.IntegerType()),
                    t.StructField("beta", t.DoubleType()),
                    t.StructField("standardError", t.DoubleType()),
                ]
            )
        )
    )
    quality_controls = implements(QualityControls.quality_controls)
