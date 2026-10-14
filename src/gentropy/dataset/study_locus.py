"""Study locus: the staging result of clumping, one row per locus.

A study locus is a region of one study that clumping found worth fine-mapping. It is
produced by locus-breaker, window and LD clumping, consumed by fine-mapping, and
never released. Its key ``id`` is ``md5(studyId, chromosome, locusStart, locusEnd)``.

Its lead variant is the variant clumping used to define the locus. ``studyId`` is
the key into the study index and the summary statistics, and ``leadVariantId`` is
both the lead and the key into the variant index. ``ldSet`` holds the variants
correlated with the lead.

A study locus comes in three states, which share their columns through
``_StudyLocusColumns``:

* ``StudyLocus``, as clumping produces it: the region and its lead.
* ``AnnotatedStudyLocus``, returned by ``gentropy.method.annotation.annotate_locus``: the same columns
  plus ``locus``, the statistics of every variant the study measured inside the
  region. Fine-mapping needs ``locus``, so it asks for the ``LocusAnnotated``
  trait, and the type checker rejects a study locus that was never annotated.
* ``LDAnnotatedStudyLocus``, returned by ``gentropy.method.annotation.annotate_ld``:
  the annotated locus with ``ldSet`` filled from the LD reference panel. LD
  clumping asks for the ``LDAnnotated`` trait.

``ldSet`` is optional in the first two states and mandatory in the last, so
each state declares it itself rather than sharing it through the mixin.

Other datasets refer to a study locus through the ``HasStudyLocusId`` trait.
They store its field under their own column name; ``StudyLocus`` itself stores it
as ``id``. Code reads the key as ``HasStudyLocusId.of(ds).study_locus_id``,
which resolves to ``id`` here and to the other dataset's column elsewhere. Code
written for ``StudyLocus`` itself reads columns as attributes, such as
``loci.lead_variant_id``.

Examples:
    >>> StudyLocus.print_schema()
    StudyLocus
     |-- id: string (mandatory: HasStudyLocusId.study_locus_id)
     |-- studyId: string (mandatory: HasStudyId.study_id)
     |-- chromosome: string (mandatory: Region.chromosome)
     |-- locusStart: int (mandatory: Region.locus_start)
     |-- locusEnd: int (mandatory: Region.locus_end)
     |-- leadVariantId: string (mandatory: LeadVariant.variant_id, HasVariantId.variant_id)
     |-- qualityControls: array<string> (mandatory: QualityControls.quality_controls)
     |-- ldSet: array (optional)
     |    |-- element: struct
     |    |    |-- tagVariantId: string
     |    |    |-- r2Overall: float
    >>> AnnotatedStudyLocus.print_schema()
    AnnotatedStudyLocus
     |-- id: string (mandatory: HasStudyLocusId.study_locus_id)
     |-- studyId: string (mandatory: HasStudyId.study_id)
     |-- chromosome: string (mandatory: Region.chromosome)
     |-- locusStart: int (mandatory: Region.locus_start)
     |-- locusEnd: int (mandatory: Region.locus_end)
     |-- leadVariantId: string (mandatory: LeadVariant.variant_id, HasVariantId.variant_id)
     |-- qualityControls: array<string> (mandatory: QualityControls.quality_controls)
     |-- locus: array (mandatory: LocusAnnotated.locus)
     |    |-- element: struct
     |    |    |-- variantId: string
     |    |    |-- pValueMantissa: float
     |    |    |-- pValueExponent: int
     |    |    |-- effectAlleleFrequencyFromSource: float
     |    |    |-- sampleSize: int
     |    |    |-- beta: double
     |    |    |-- standardError: double
     |-- ldSet: array (optional)
     |    |-- element: struct
     |    |    |-- tagVariantId: string
     |    |    |-- r2Overall: float
    >>> [
    ...     (field.name, field.metadata.get("traits"))
    ...     for field in LDAnnotatedStudyLocus.schema.fields[-2:]
    ... ]
    [('locus', ['LocusAnnotated.locus']), ('ldSet', ['LDAnnotated.ld_set'])]

    One locus annotated with LD and one without:

    >>> ld_set = [("1_100501_G_A", 0.5)]
    >>> rows = [
    ...     ("0feec5fa", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], ld_set),
    ...     ("991ffbcc", "GCST1", "2", 100_000, 500_000, "2_100500_C_T", [], None),
    ... ]
    >>> loci = StudyLocus(spark.createDataFrame(rows, StudyLocus.schema))
    >>> loci.df.select(
    ...     HasStudyLocusId.of(loci).study_locus_id,
    ...     loci.lead_variant_id,
    ...     loci.ld_set,
    ... ).show(truncate=False)
    +--------+-------------+---------------------+
    |id      |leadVariantId|ldSet                |
    +--------+-------------+---------------------+
    |0feec5fa|1_100500_C_T |[{1_100501_G_A, 0.5}]|
    |991ffbcc|2_100500_C_T |NULL                 |
    +--------+-------------+---------------------+
    <BLANKLINE>

    The key is generated from ``ID_COLUMNS``:

    >>> import hashlib
    >>> StudyLocus.ID_COLUMNS
    ('studyId', 'chromosome', 'locusStart', 'locusEnd')
    >>> key = loci.df.select(StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS)).first()[0]
    >>> key == hashlib.md5(b"GCST11100000500000").hexdigest()
    True

    Staging quality control flags are written to ``qualityControls`` by value:

    >>> StudyLocusQualityFlag.WINDOW_CLUMPED.value
    'Explained by a more significant variant in the same window'
    >>> [flag.name for flag in StudyLocusQualityFlag]
    ['WINDOW_CLUMPED', 'LD_CLUMPED', 'NO_POPULATION', 'UNRESOLVED_LD']
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar, TypeAlias

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements, optional
from gentropy.model.trait.identifier import (
    HasStudyId,
    HasStudyLocusId,
    HasVariantId,
)
from gentropy.model.trait.locus import (
    LDAnnotated,
    LeadVariant,
    LocusAnnotated,
    Region,
)
from gentropy.model.trait.quality_control import QualityControls

StudyLocusTraits: TypeAlias = (
    HasStudyLocusId | HasStudyId | HasVariantId | Region | LeadVariant | QualityControls
)
"""The traits of every study locus, annotated or not."""


class StudyLocusQualityFlag(Enum):
    """Quality control flags.

    Attributes:
        WINDOW_CLUMPED (str): Explained by a more significant variant in the same window
        LD_CLUMPED (str): Explained by a more significant variant in high LD
        NO_POPULATION (str): Study does not have population annotation to resolve LD
        UNRESOLVED_LD (str): Variant not found in LD reference
    """

    WINDOW_CLUMPED = "Explained by a more significant variant in the same window"
    LD_CLUMPED = "Explained by a more significant variant in high LD"
    NO_POPULATION = "Study does not have population annotation to resolve LD"
    UNRESOLVED_LD = "Variant not found in LD reference"


class _StudyLocusColumns:
    """The columns of every study locus, shared by both states."""

    ID_COLUMNS: ClassVar[tuple[str, ...]] = (
        HasStudyId.study_id.name,
        Region.chromosome.name,
        Region.locus_start.name,
        Region.locus_end.name,
    )
    """Columns whose values define the key ``id``, hashed by ``generate_identifier``."""

    id = implements(HasStudyLocusId.study_locus_id)
    study_id = implements(HasStudyId.study_id)
    chromosome = implements(Region.chromosome)
    locus_start = implements(Region.locus_start)
    locus_end = implements(Region.locus_end)
    lead_variant_id = implements(LeadVariant.variant_id, HasVariantId.variant_id)
    quality_controls = implements(QualityControls.quality_controls)


class StudyLocus(_StudyLocusColumns, Dataset[StudyLocusTraits]):
    """Clumped region of one study, staged for fine-mapping."""

    ld_set = optional(LDAnnotated.ld_set.dtype)


class AnnotatedStudyLocus(
    _StudyLocusColumns, Dataset[StudyLocusTraits | LocusAnnotated]
):
    """A study locus with the statistics of every variant inside its region."""

    locus = implements(LocusAnnotated.locus)
    ld_set = optional(LDAnnotated.ld_set.dtype)


class LDAnnotatedStudyLocus(
    _StudyLocusColumns, Dataset[StudyLocusTraits | LocusAnnotated | LDAnnotated]
):
    """An annotated study locus whose lead carries its LD tags in ``ldSet``."""

    locus = implements(LocusAnnotated.locus)
    ld_set = implements(LDAnnotated.ld_set)
