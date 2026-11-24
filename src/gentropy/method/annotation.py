"""Annotate study loci with the statistics and the LD of their variants.

Two functions add a column to a study locus and return it in its next state:

* ``annotate_locus`` collects the summary statistics inside each locus into
  ``locus``, turning a ``StudyLocus`` into an ``AnnotatedStudyLocus``.
* ``annotate_ld`` fills ``ldSet`` with the variants correlated with each lead,
  turning an ``AnnotatedStudyLocus`` into an ``LDAnnotatedStudyLocus``.

LD comes second because LD clumping needs both: ``ldSet`` to find linked leads,
and ``locus`` for the lead's p-value.

``keep_lead_entry`` reduces ``locus`` to the lead's own entry, the only entry the
LD annotation, the LD clumping and the posteriors of PICS read.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.ld import major_population
from gentropy.dataset.study_locus import (
    AnnotatedStudyLocus,
    LDAnnotatedStudyLocus,
    StudyLocus,
    StudyLocusQualityCheck,
)

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.dataset.ld_index import LDIndex
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.summary_statistics import SummaryStatistics

# How far above 1 a panel r2 may drift from rounding and still be read as 1.
_R2_TOLERANCE = 1e-6

# A study locus state with ``locus``: annotated, or LD-annotated.
_Loci = TypeVar("_Loci", bound=AnnotatedStudyLocus)


def keep_lead_entry(loci: _Loci) -> _Loci:
    """Reduce ``locus`` to the lead's own entry.

    The LD annotation, the LD clumping and the posteriors of PICS read the lead's
    entry of ``locus`` only, for its p-value. On summary statistics ``locus``
    holds thousands of entries per locus; with the lead's entry only, these
    steps neither shuffle nor cache them. A locus whose lead has no entry gets
    an empty ``locus``.

    Args:
        loci (_Loci): The loci, annotated or LD-annotated.

    Returns:
        _Loci: The same loci, as the same dataset type, with the lead's entry in ``locus``.

    Examples:
        >>> def entry(variant, exponent):
        ...     return (variant, 5.0, exponent, 0.3, 1000, 0.1, 0.01)
        >>> rows = [("L1", "S1", "1", 1, 1000, "A", [], [entry("A", -8), entry("B", -7)])]
        >>> loci = AnnotatedStudyLocus(
        ...     spark.createDataFrame(rows, AnnotatedStudyLocus.get_schema()),
        ...     AnnotatedStudyLocus.get_schema(),
        ... )
        >>> lead_only = keep_lead_entry(loci)
        >>> type(lead_only).__name__, [e.variantId for e in lead_only.df.first().locus]
        ('AnnotatedStudyLocus', ['A'])
    """
    lead = f.col("leadVariantId")
    return type(loci)(
        _df=loci.df.withColumn(
            "locus", f.filter("locus", lambda e: e["variantId"] == lead)
        ),
        _schema=loci.schema,
    )


def annotate_locus(
    loci: StudyLocus, summary_statistics: SummaryStatistics
) -> AnnotatedStudyLocus:
    """Collect the summary statistics inside each locus into ``locus``.

    An association belongs to a locus when it has the same study and
    chromosome and its position lies between ``locusStart`` and ``locusEnd``,
    both included. Only the bounds decide, so loci from any clumping method
    are collected the same way. The entries are sorted by position, and a
    locus with no association inside gets an empty array.

    Rows that share an ``id``, such as a lead and the variants clumped into
    it, describe one locus: it is collected once and given to each of them.
    The loci are broadcast to the join, as there are far fewer of them than
    associations.

    ``locus`` is meant to hold everything the study measured: summary
    statistics built from curated top hits give a locus of a few rows that
    looks complete.

    Args:
        loci (StudyLocus): The loci to annotate.
        summary_statistics (SummaryStatistics): The statistics to collect.

    Returns:
        AnnotatedStudyLocus: The same rows, with ``locus`` filled.

    Examples:
        >>> from gentropy.dataset.summary_statistics import SummaryStatistics
        >>> loci = StudyLocus(
        ...     spark.createDataFrame(
        ...         [("L1", "GCST1", "1", 100, 300, "1_200_A_T", [])],
        ...         StudyLocus.get_schema(),
        ...     )
        ... )
        >>> rows = [
        ...     ("GCST1", "1_250_C_G", "1", 250, 0.1, 5000, 2.0, -9, 0.3, 0.01),
        ...     ("GCST1", "1_200_A_T", "1", 200, 0.2, 5000, 1.0, -10, 0.3, 0.01),
        ...     ("GCST1", "1_900_G_A", "1", 900, 0.1, 5000, 1.0, -8, 0.3, 0.01),
        ... ]
        >>> summary_statistics = SummaryStatistics(
        ...     spark.createDataFrame(rows, SummaryStatistics.get_schema())
        ... )
        >>> annotated = annotate_locus(loci, summary_statistics)
        >>> type(annotated).__name__
        'AnnotatedStudyLocus'
        >>> (row,) = annotated.df.select("id", "locus").collect()
        >>> row.id, [entry.variantId for entry in row.locus]
        ('L1', ['1_200_A_T', '1_250_C_G'])
        >>> row.locus[0]
        Row(variantId='1_200_A_T', pValueMantissa=1.0, pValueExponent=-10, effectAlleleFrequencyFromSource=0.30000001192092896, sampleSize=5000, beta=0.2, standardError=0.01)
    """
    schema = AnnotatedStudyLocus.get_schema()
    # One row per association: where it is, and its entry in a locus.
    entries = summary_statistics.df.select(
        f.col("studyId").alias("entryStudyId"),
        f.col("chromosome").alias("entryChromosome"),
        "position",
        f.struct(
            "variantId",
            "pValueMantissa",
            "pValueExponent",
            "effectAlleleFrequencyFromSource",
            "sampleSize",
            "beta",
            "standardError",
        ).alias("locus"),
    )
    # One row per locus, however many rows share its id.
    bounds = loci.df.select(
        "id", "studyId", "chromosome", "locusStart", "locusEnd"
    ).distinct()
    collected = (
        entries.join(
            f.broadcast(bounds),
            on=(f.col("entryStudyId") == f.col("studyId"))
            & (f.col("entryChromosome") == f.col("chromosome"))
            & f.col("position").between(f.col("locusStart"), f.col("locusEnd")),
        )
        .groupBy("id")
        .agg(
            # Sorting (position, entry) pairs orders the entries along the locus:
            f.transform(
                f.array_sort(f.collect_list(f.struct("position", "locus"))),
                lambda pair: pair["locus"],
            ).alias("locus")
        )
    )
    annotated = loci.df.join(collected, on="id", how="left").withColumn(
        "locus", f.coalesce(f.col("locus"), f.array().cast(schema["locus"].dataType))
    )
    return AnnotatedStudyLocus(
        _df=annotated.select(
            *[name for name in schema.fieldNames() if name in annotated.columns]
        ),
        _schema=schema,
    )


def annotate_ld(
    loci: AnnotatedStudyLocus,
    study_index: StudyIndex,
    ld_index: LDIndex,
    r2_threshold: float = 0.5,
) -> LDAnnotatedStudyLocus:
    """Fill ``ldSet`` with the variants correlated with each lead.

    The lead is looked up in the LD index on its variant and chromosome. The
    panel gives ``r`` for each of its populations, and the study decides
    which one counts: the population with the largest share of its samples
    (see ``major_population``). ``r2Overall`` is that population's ``r``
    squared, or 0 when the panel has no ``r`` for it. An ``r2`` that rounding
    lifts at most 1e-6 above 1 is read as 1. Tags with
    ``r2Overall`` at or above ``r2_threshold`` are kept.

    The index holds the pairs at r2 0.5 or more in at least one of its
    populations, so a pair can be in the index with no ``r`` for the study's
    population and an ``r2Overall`` of 0. The default threshold of 0.5 keeps
    only tags in LD in the study's own population. Legacy annotated at 0.0,
    which also kept those zero tags, so a lead in LD with a stronger lead only
    in another population was clumped, and PICS received tags at r2 0.

    Every lead ends up with an ``ldSet``. When nothing is kept, because the
    lead is not in the panel, the study has no population, or no tag passes
    the threshold, the row is flagged ``UNRESOLVED_LD`` and the lead is its
    only tag. A study with no population is also flagged ``NO_POPULATION``.
    The lead's own ``r2Overall`` is always 1. Loci that already carry an ``ldSet``,
    such as an ``LDAnnotatedStudyLocus``, have it replaced, and ``locus`` is kept as it is.

    Args:
        loci (AnnotatedStudyLocus): The annotated loci.
        study_index (StudyIndex): The studies' ancestries; one row per study.
        ld_index (LDIndex): The LD reference panel.
        r2_threshold (float): Smallest ``r2Overall`` kept. Defaults to 0.5.

    Returns:
        LDAnnotatedStudyLocus: The same rows, with ``ldSet`` filled.

    Examples:
        >>> from gentropy.dataset.ld_index import LDIndex
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> loci = AnnotatedStudyLocus(
        ...     spark.createDataFrame(
        ...         [("L1", "GCST1", "1", 100, 300, "1_200_A_T", [], [])],
        ...         AnnotatedStudyLocus.get_schema(),
        ...     )
        ... )
        >>> study_index = StudyIndex(
        ...     spark.createDataFrame(
        ...         [("GCST1", "GCST", "gwas", [("nfe", 0.9), ("afr", 0.1)])],
        ...         "studyId string, projectId string, studyType string, "
        ...         "ldPopulationStructure array<struct<ldPopulation: string, "
        ...         "relativeSampleSize: double>>",
        ...     )
        ... )
        >>> ld_set = [
        ...     ("1_200_A_T", [("nfe", 1.0), ("afr", 1.0)]),
        ...     ("1_210_C_G", [("nfe", 0.8), ("afr", 0.3)]),
        ...     ("1_220_G_A", [("nfe", 0.6), ("afr", 0.9)]),
        ... ]
        >>> ld_index = LDIndex(
        ...     spark.createDataFrame([("1_200_A_T", "1", ld_set)], LDIndex.get_schema())
        ... )
        >>> annotated = annotate_ld(loci, study_index, ld_index)
        >>> type(annotated).__name__
        'LDAnnotatedStudyLocus'

        nfe is the major population, so the second tag is kept at 0.8² and
        the third, at 0.6² = 0.36, falls below the default threshold:

        >>> row = annotated.df.select("ldSet", "qualityControls").first()
        >>> [(tag.tagVariantId, round(tag.r2Overall, 2)) for tag in row.ldSet]
        [('1_200_A_T', 1.0), ('1_210_C_G', 0.64)]
        >>> row.qualityControls
        []
    """
    schema = LDAnnotatedStudyLocus.get_schema()
    lead = f.col("leadVariantId")
    major = major_population(f.col("ldPopulationStructure"))

    def r2_overall(tag: Column) -> Column:
        """Return the r2 of a tag in the major population: 0 for a tag measured without it.

        Args:
            tag (Column): A tag of the LD set of the panel.

        Returns:
            Column: The r2 of the tag in the major population.
        """
        r = f.get(f.filter(tag["rValues"], lambda r: r["population"] == major), 0)
        r2 = f.coalesce(f.pow(r["r"], 2), f.lit(0.0))
        # A panel r a few ulps past ±1 squares above 1, which PICS cannot take.
        return f.when(r2.between(1.0, 1.0 + _R2_TOLERANCE), f.lit(1.0)).otherwise(r2)

    tags = f.transform(
        f.col("panelLdSet"),
        lambda tag: f.struct(
            tag["tagVariantId"].alias("tagVariantId"),
            r2_overall(tag).cast(t.FloatType()).alias("r2Overall"),
        ),
    )
    kept = f.filter(
        f.when(f.col("ldPopulationStructure").isNotNull(), tags),
        lambda tag: tag["r2Overall"] >= r2_threshold,
    )
    # size(null) is -1, so a missing set stays null as well.
    resolved = f.when(f.size(kept) > 0, kept)
    lead_only = f.array(
        f.struct(
            lead.alias("tagVariantId"),
            f.lit(1.0).cast(t.FloatType()).alias("r2Overall"),
        )
    )

    # The LD sets of the leads only. The leads are broadcast, so the LD index is filtered
    # where it is read: the left join below cannot broadcast the LD index, its preserved
    # side being the loci, and would otherwise shuffle the whole LD index.
    leads = (
        loci.df.select("leadVariantId", "chromosome")
        .where(f.col("leadVariantId").isNotNull())
        .distinct()
    )
    lead_ld_sets = ld_index.df.select(
        f.col("variantId").alias("leadVariantId"),
        "chromosome",
        f.col("ldSet").alias("panelLdSet"),
    ).join(f.broadcast(leads), on=["leadVariantId", "chromosome"], how="left_semi")

    annotated = (
        # An LDAnnotatedStudyLocus is also an AnnotatedStudyLocus: replace its ldSet.
        loci.df.drop("ldSet")
        .join(
            study_index.df.select("studyId", "ldPopulationStructure"),
            on="studyId",
            how="left",
        )
        .join(
            lead_ld_sets,
            on=["leadVariantId", "chromosome"],
            how="left",
        )
        .withColumn(
            "qualityControls",
            StudyLocus.update_quality_flag(
                StudyLocus.update_quality_flag(
                    f.col("qualityControls"),
                    resolved.isNull(),
                    StudyLocusQualityCheck.UNRESOLVED_LD,
                ),
                f.col("ldPopulationStructure").isNull(),
                StudyLocusQualityCheck.NO_POPULATION,
            ),
        )
        .withColumn(
            "ldSet",
            f.transform(
                f.coalesce(resolved, f.when(lead.isNotNull(), lead_only)),
                lambda tag: f.when(tag["tagVariantId"] == lead, lead_only[0]).otherwise(
                    tag
                ),
            ).cast(schema["ldSet"].dataType),
        )
    )
    return LDAnnotatedStudyLocus(
        _df=annotated.select(*schema.fieldNames()), _schema=schema
    )
