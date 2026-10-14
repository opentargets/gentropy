"""Annotate study loci with the statistics and the LD of their variants.

Both functions add a column to a study locus and return it in its next state:

* ``annotate_locus`` collects the associations inside each locus into ``locus``,
  turning a ``StudyLocus`` into an ``AnnotatedStudyLocus``.
* ``annotate_ld`` fills ``ldSet`` with the variants correlated with each lead,
  turning an ``AnnotatedStudyLocus`` into an ``LDAnnotatedStudyLocus``.

LD comes second because LD clumping needs both: ``ldSet`` to find linked leads,
and ``locus`` for the lead's p-value.
"""

from __future__ import annotations

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.ld import major_population
from gentropy.common.quality_control import add_flag
from gentropy.common.spark import element_struct
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_locus import (
    AnnotatedStudyLocus,
    LDAnnotatedStudyLocus,
    StudyLocus,
    StudyLocusQualityFlag,
)
from gentropy.model.core.dataset import Dataset
from gentropy.model.trait.association import Association
from gentropy.model.trait.identifier import HasStudyId
from gentropy.model.trait.locus import LDAnnotated, LocusAnnotated, VariantLocation
from gentropy.model.trait.study import LDPopulationStructure

_TAG, _R2 = element_struct(LDAnnotated.ld_set.dtype).fieldNames()
"""``ldSet`` fields of a study locus: the tag and its r2 in the study's population."""

_PANEL_TAG, _R_VALUES = element_struct(LDIndex.ld_set.dtype).fieldNames()
"""``ldSet`` fields of the LD index: the tag and its r in each population."""

_R_POPULATION, _R = element_struct(
    element_struct(LDIndex.ld_set.dtype)[_R_VALUES].dataType
).fieldNames()
"""``rValues`` fields of the LD index: the population and r measured in it."""


def annotate_locus(
    loci: StudyLocus, associations: Dataset[HasStudyId | VariantLocation | Association]
) -> AnnotatedStudyLocus:
    """Collect the associations inside each locus into ``locus``.

    An association belongs to a locus when it has the same study and
    chromosome and its position lies between ``locusStart`` and ``locusEnd``,
    both included. Only the bounds decide, so loci from any clumping method
    are collected the same way. The entries are sorted by position, and a
    locus with no association inside gets an empty array.

    Rows that share an ``id``, such as a lead and the variants clumped into
    it, describe one locus: it is collected once and given to each of them.
    The loci are broadcast to the join, as there are far fewer of them than
    associations.

    Any dataset with the three traits can be collected from, but ``locus`` is
    meant to hold everything the study measured, so pass summary statistics:
    curated top hits would give a locus of a few rows that looks complete.

    Args:
        loci (StudyLocus): The loci to annotate.
        associations (Dataset[HasStudyId | VariantLocation | Association]): The
            statistics to collect, read through their traits.

    Returns:
        AnnotatedStudyLocus: The same rows, with ``locus`` filled.

    Examples:
        >>> from gentropy.dataset.summary_statistics import SummaryStatistics
        >>> loci = StudyLocus(
        ...     spark.createDataFrame(
        ...         [("L1", "GCST1", "1", 100, 300, "1_200_A_T", [], None)],
        ...         StudyLocus.schema,
        ...     )
        ... )
        >>> rows = [
        ...     ("GCST1", "1_250_C_G", "1", 250, 0.1, 5000, 2.0, -9, 0.3, 0.01),
        ...     ("GCST1", "1_200_A_T", "1", 200, 0.2, 5000, 1.0, -10, 0.3, 0.01),
        ...     ("GCST1", "1_900_G_A", "1", 900, 0.1, 5000, 1.0, -8, 0.3, 0.01),
        ... ]
        >>> summary_statistics = SummaryStatistics(
        ...     spark.createDataFrame(rows, SummaryStatistics.schema)
        ... )
        >>> annotated = annotate_locus(loci, summary_statistics)
        >>> type(annotated).__name__
        'AnnotatedStudyLocus'
        >>> (row,) = annotated.df.select(annotated.id, annotated.locus).collect()
        >>> row.id, [entry.variantId for entry in row.locus]
        ('L1', ['1_200_A_T', '1_250_C_G'])
        >>> row.locus[0]
        Row(variantId='1_200_A_T', pValueMantissa=1.0, pValueExponent=-10, effectAlleleFrequencyFromSource=0.30000001192092896, sampleSize=5000, beta=0.2, standardError=0.01)
    """
    study = HasStudyId.of(associations)
    location = VariantLocation.of(associations)
    association = Association.of(associations)
    position = VariantLocation.position.name
    locus = LocusAnnotated.locus.name

    # One row per association: where it is, and its entry in a locus.
    entries = associations.df.select(
        study.study_id.alias(HasStudyId.study_id.name),
        location.chromosome.alias(VariantLocation.chromosome.name),
        location.position.alias(position),
        f.struct(
            association.variant_id.alias(Association.variant_id.name),
            association.p_value_mantissa.alias(Association.p_value_mantissa.name),
            association.p_value_exponent.alias(Association.p_value_exponent.name),
            association.effect_allele_frequency_from_source.alias(
                Association.effect_allele_frequency_from_source.name
            ),
            association.sample_size.alias(Association.sample_size.name),
            association.beta.alias(Association.beta.name),
            association.standard_error.alias(Association.standard_error.name),
        ).alias(locus),
    )
    # One row per locus, however many rows share its id.
    bounds = loci.df.select(
        loci.id, loci.study_id, loci.chromosome, loci.locus_start, loci.locus_end
    ).distinct()
    collected = (
        entries.join(
            f.broadcast(bounds),
            on=(entries[HasStudyId.study_id.name] == bounds[StudyLocus.study_id.name])
            & (
                entries[VariantLocation.chromosome.name]
                == bounds[StudyLocus.chromosome.name]
            )
            & entries[position].between(
                bounds[StudyLocus.locus_start.name],
                bounds[StudyLocus.locus_end.name],
            ),
        )
        .groupBy(bounds[StudyLocus.id.name])
        .agg(
            # Sorting (position, entry) pairs orders the entries along the locus:
            f.transform(
                f.array_sort(
                    f.collect_list(f.struct(entries[position], entries[locus]))
                ),
                lambda pair: pair[locus],
            ).alias(locus)
        )
    )
    annotated = loci.df.join(collected, on=StudyLocus.id.name, how="left").withColumn(
        locus, f.coalesce(f.col(locus), f.array().cast(LocusAnnotated.locus.dtype))
    )
    return AnnotatedStudyLocus(
        annotated.select(
            *[
                name
                for name in AnnotatedStudyLocus.schema.fieldNames()
                if name in annotated.columns
            ]
        )
    )


def annotate_ld(
    loci: AnnotatedStudyLocus,
    studies: Dataset[HasStudyId | LDPopulationStructure],
    ld_index: LDIndex,
    r2_threshold: float = 0.5,
) -> LDAnnotatedStudyLocus:
    """Fill ``ldSet`` with the variants correlated with each lead.

    The lead is looked up in the LD index on its variant and chromosome. The
    panel gives ``r`` for each of its populations, and the study decides
    which one counts: the population with the largest share of its samples
    (see ``major_population``). ``r2Overall`` is that population's ``r``
    squared, or 0 when the panel has no ``r`` for it. Tags with
    ``r2Overall`` at or above ``r2_threshold`` are kept; the index is built
    at r2 0.5, so a lower threshold keeps nothing more.

    Every lead ends up with an ``ldSet``. When nothing is kept, because the
    lead is not in the panel, the study has no population, or no tag passes
    the threshold, the row is flagged ``UNRESOLVED_LD`` and the lead is its
    only tag. A study with no population is also flagged ``NO_POPULATION``.
    The lead's own ``r2Overall`` is always 1. Any ``ldSet`` already on the
    rows is replaced, and ``locus`` is kept as it is.

    Args:
        loci (AnnotatedStudyLocus): The annotated loci.
        studies (Dataset[HasStudyId | LDPopulationStructure]): The studies'
            ancestries, read through their traits; one row per study.
        ld_index (LDIndex): The LD reference panel.
        r2_threshold (float): Smallest ``r2Overall`` kept. Defaults to 0.5.

    Returns:
        LDAnnotatedStudyLocus: The same rows, with ``ldSet`` filled.

    Examples:
        >>> from gentropy.dataset.ld_index import LDIndex
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> loci = AnnotatedStudyLocus(
        ...     spark.createDataFrame(
        ...         [("L1", "GCST1", "1", 100, 300, "1_200_A_T", [], [], None)],
        ...         AnnotatedStudyLocus.schema,
        ...     )
        ... )
        >>> studies = StudyIndex(
        ...     spark.createDataFrame(
        ...         [("GCST1", "GCST", "gwas", [("nfe", 0.9), ("afr", 0.1)], [])],
        ...         "studyId string, projectId string, studyType string, "
        ...         "ldPopulationStructure array<struct<ldPopulation: string, "
        ...         "relativeSampleSize: double>>, qualityControls array<string>",
        ...     )
        ... )
        >>> ld_set = [
        ...     ("1_200_A_T", [("nfe", 1.0), ("afr", 1.0)]),
        ...     ("1_210_C_G", [("nfe", 0.8), ("afr", 0.3)]),
        ...     ("1_220_G_A", [("nfe", 0.6), ("afr", 0.9)]),
        ... ]
        >>> ld_index = LDIndex(
        ...     spark.createDataFrame([("1_200_A_T", "1", ld_set)], LDIndex.schema)
        ... )
        >>> annotated = annotate_ld(loci, studies, ld_index)
        >>> type(annotated).__name__
        'LDAnnotatedStudyLocus'

        nfe is the major population, so the second tag is kept at 0.8² and
        the third, at 0.6² = 0.36, falls below the default threshold:

        >>> row = annotated.df.select(annotated.ld_set, annotated.quality_controls).first()
        >>> [(tag.tagVariantId, round(tag.r2Overall, 2)) for tag in row.ldSet]
        [('1_200_A_T', 1.0), ('1_210_C_G', 0.64)]
        >>> row.qualityControls
        []
    """
    ld_set = LDAnnotated.ld_set.name
    structure = LDPopulationStructure.ld_population_structure.name
    lead = f.col(StudyLocus.lead_variant_id.name)
    study = HasStudyId.of(studies)
    populations = LDPopulationStructure.of(studies)

    major = major_population(f.col(structure))
    # r2 in the major population; a tag measured without it counts as 0.
    tags = f.transform(
        f.col(LDIndex.ld_set.name),
        lambda tag: f.struct(
            tag[_PANEL_TAG].alias(_TAG),
            f.coalesce(
                f.pow(
                    f.get(
                        f.filter(tag[_R_VALUES], lambda r: r[_R_POPULATION] == major),
                        0,
                    )[_R],
                    2,
                ),
                f.lit(0.0),
            )
            .cast(t.FloatType())
            .alias(_R2),
        ),
    )
    kept = f.filter(
        f.when(f.col(structure).isNotNull(), tags),
        lambda tag: tag[_R2] >= r2_threshold,
    )
    # size(null) is -1, so a missing set stays null as well.
    resolved = f.when(f.size(kept) > 0, kept)
    lead_only = f.array(
        f.struct(
            lead.alias(_TAG),
            f.lit(1.0).cast(t.FloatType()).alias(_R2),
        )
    )

    annotated = (
        loci.df.drop(ld_set)
        .join(
            studies.df.select(
                study.study_id.alias(StudyLocus.study_id.name),
                populations.ld_population_structure.alias(structure),
            ),
            on=StudyLocus.study_id.name,
            how="left",
        )
        .join(
            ld_index.df.select(
                ld_index.variant_id.alias(StudyLocus.lead_variant_id.name),
                ld_index.chromosome.alias(StudyLocus.chromosome.name),
                ld_index.ld_set,
            ),
            on=[StudyLocus.lead_variant_id.name, StudyLocus.chromosome.name],
            how="left",
        )
        .withColumn(
            StudyLocus.quality_controls.name,
            add_flag(
                add_flag(
                    f.col(StudyLocus.quality_controls.name),
                    resolved.isNull(),
                    StudyLocusQualityFlag.UNRESOLVED_LD,
                ),
                f.col(structure).isNull(),
                StudyLocusQualityFlag.NO_POPULATION,
            ),
        )
        .withColumn(
            ld_set,
            f.transform(
                f.coalesce(resolved, f.when(lead.isNotNull(), lead_only)),
                lambda tag: f.when(tag[_TAG] == lead, lead_only[0]).otherwise(tag),
            ).cast(LDAnnotated.ld_set.dtype),
        )
    )
    return LDAnnotatedStudyLocus(
        annotated.select(*LDAnnotatedStudyLocus.schema.fieldNames())
    )
