"""Study index: one row per study, describing what was measured and in whom.

A study is one association analysis: a GWAS of one trait, or a molecular QTL
study of one gene in one biosample. The index records the trait or gene, the
publication, the samples and their ancestries, where the summary statistics
are, and the quality control flags raised on the study.

``studyId`` is the key other datasets join on, and also partitions the summary
statistics. ``projectId``, ``studyType`` and ``qualityControls`` are mandatory;
every other column is optional and may be left out of a DataFrame.

The layout is the legacy one with one rename: ``sumstatQCValues`` is now
``sumstatQcValues``, because column names follow the camelCase form of their
snake_case attribute and that rule writes acronyms in lower case.

Examples:
    >>> StudyIndex.print_schema()
    StudyIndex
     |-- studyId: string (mandatory: CanJoinToStudyIndex.study_id, CanJoinToSummaryStatistics.study_id)
     |-- projectId: string (mandatory: HasProjectId.project_id)
     |-- studyType: string (mandatory: HasStudyType.study_type)
     |-- traitFromSource: string (optional)
     |-- traitFromSourceMappedIds: array<string> (optional)
     |-- diseaseIds: array<string> (optional)
     |-- geneId: string (optional)
     |-- biosampleFromSourceId: string (optional)
     |-- biosampleId: string (optional)
     |-- pubmedId: string (optional)
     |-- publicationTitle: string (optional)
     |-- publicationFirstAuthor: string (optional)
     |-- publicationDate: string (optional)
     |-- publicationJournal: string (optional)
     |-- backgroundTraitFromSourceMappedIds: array<string> (optional)
     |-- backgroundDiseaseIds: array<string> (optional)
     |-- initialSampleSize: string (optional)
     |-- nCases: int (optional)
     |-- nControls: int (optional)
     |-- nSamples: int (optional)
     |-- cohorts: array<string> (optional)
     |-- ldPopulationStructure: array (optional)
     |    |-- element: struct
     |    |    |-- ldPopulation: string
     |    |    |-- relativeSampleSize: double
     |-- discoverySamples: array (optional)
     |    |-- element: struct
     |    |    |-- sampleSize: int
     |    |    |-- ancestry: string
     |-- replicationSamples: array (optional)
     |    |-- element: struct
     |    |    |-- sampleSize: int
     |    |    |-- ancestry: string
     |-- qualityControls: array<string> (mandatory: QualityControls.quality_controls)
     |-- analysisFlags: array<string> (optional)
     |-- summarystatsLocation: string (optional)
     |-- hasSumstats: boolean (optional)
     |-- condition: string (optional)
     |-- sumstatQcValues: array (optional)
     |    |-- element: struct
     |    |    |-- QCCheckName: string
     |    |    |-- QCCheckValue: float

    A GWAS described by its mandatory columns and two optional ones:

    >>> studies = StudyIndex(
    ...     spark.createDataFrame(
    ...         [("GCST1", "GCST", "gwas", [], "Asthma", 5000)],
    ...         "studyId string, projectId string, studyType string, "
    ...         "qualityControls array<string>, traitFromSource string, nSamples int",
    ...     )
    ... )
    >>> studies.df.select(studies.study_id, studies.trait_from_source).show()
    +-------+---------------+
    |studyId|traitFromSource|
    +-------+---------------+
    |  GCST1|         Asthma|
    +-------+---------------+
    <BLANKLINE>
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements, optional
from gentropy.model.trait.join import CanJoinToStudyIndex, CanJoinToSummaryStatistics
from gentropy.model.trait.quality_control import QualityControls
from gentropy.model.trait.study import HasProjectId, HasStudyType

_SAMPLES = t.ArrayType(
    t.StructType(
        [
            t.StructField("sampleSize", t.IntegerType()),
            t.StructField("ancestry", t.StringType()),
        ]
    ),
    containsNull=False,
)
"""Sample sizes by ancestry, shared by the discovery and replication stages."""


class StudyIndex(
    Dataset[
        CanJoinToStudyIndex
        | CanJoinToSummaryStatistics
        | HasProjectId
        | HasStudyType
        | QualityControls
    ]
):
    """One row per study: what was measured, in whom, and where the results are."""

    study_id = implements(
        CanJoinToStudyIndex.study_id, CanJoinToSummaryStatistics.study_id
    )
    project_id = implements(HasProjectId.project_id)
    study_type = implements(HasStudyType.study_type)
    trait_from_source = optional(t.StringType())
    trait_from_source_mapped_ids = optional(t.ArrayType(t.StringType()))
    disease_ids = optional(t.ArrayType(t.StringType()))
    gene_id = optional(t.StringType())
    biosample_from_source_id = optional(t.StringType())
    biosample_id = optional(t.StringType())
    pubmed_id = optional(t.StringType())
    publication_title = optional(t.StringType())
    publication_first_author = optional(t.StringType())
    publication_date = optional(t.StringType())
    publication_journal = optional(t.StringType())
    background_trait_from_source_mapped_ids = optional(t.ArrayType(t.StringType()))
    background_disease_ids = optional(t.ArrayType(t.StringType()))
    initial_sample_size = optional(t.StringType())
    n_cases = optional(t.IntegerType())
    n_controls = optional(t.IntegerType())
    n_samples = optional(t.IntegerType())
    cohorts = optional(t.ArrayType(t.StringType()))
    ld_population_structure = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("ldPopulation", t.StringType()),
                    t.StructField("relativeSampleSize", t.DoubleType()),
                ]
            ),
            containsNull=False,
        )
    )
    discovery_samples = optional(_SAMPLES)
    replication_samples = optional(_SAMPLES)
    quality_controls = implements(QualityControls.quality_controls)
    analysis_flags = optional(t.ArrayType(t.StringType()))
    summarystats_location = optional(t.StringType())
    has_sumstats = optional(t.BooleanType())
    condition = optional(t.StringType())
    sumstat_qc_values = optional(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("QCCheckName", t.StringType(), nullable=False),
                    t.StructField("QCCheckValue", t.FloatType(), nullable=False),
                ]
            )
        )
    )
