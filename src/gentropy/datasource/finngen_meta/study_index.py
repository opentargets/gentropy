"""FinnGen meta-analysis study-index data source."""

from __future__ import annotations

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.dataset.study_index import MetaAnalysisStudyIndex, StudyIndex
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import FinnGenMetaRelease, MetaAnalysisType


class FinnGenMetaManifest(MetaAnalysisStudyIndex):
    """FinnGen meta-analysis manifest."""

    @classmethod
    def from_source(
        cls,
        session: Session,
        manifest_path: str,
        meta_analysis_type: MetaAnalysisType,
        release: FinnGenMetaRelease,
        efo_mapping: EFOMapping,
    ) -> MetaAnalysisStudyIndex:
        """Load the FinnGen meta-analysis manifest from a specified path.

        The returned meta-analysis study index is annotated with
        ``traitFromSourceMappedIds`` from the supplied EFO mapping.

        Args:
            session (Session): Session object.
            manifest_path (str): Path to the manifest file.
            meta_analysis_type (MetaAnalysisType): Type of meta-analysis conducted for this release.
            release (FinnGenMetaRelease): FinnGen release identifier (e.g. ``"R12"``).
            efo_mapping (EFOMapping): EFO mapping used to annotate
                ``traitFromSourceMappedIds``.

        Returns:
            MetaAnalysisStudyIndex: Loaded manifest object.

        Raises:
            AssertionError: If the manifest file does not contain the required columns.
        """
        df = session.spark.read.csv(
            manifest_path,
            schema=meta_analysis_type.get_manifest_schema(),
            sep="\t",
            header=True,
        ).select(
            meta_analysis_type.study_id(release),
            meta_analysis_type.project_id(release),
            f.lit("gwas").alias("studyType"),
            f.lit("binary").alias("traitType"),
            f.col("name").alias("traitFromSource"),
            meta_analysis_type.n_cases(),
            meta_analysis_type.n_controls(),
            meta_analysis_type.n_samples(),
            meta_analysis_type.discovery_samples(),
            meta_analysis_type.publication_date().alias("publicationDate"),
            meta_analysis_type.initial_sample_size().alias("initialSampleSize"),
            meta_analysis_type.cohorts().alias("cohorts"),
            # Populated later when summary-statistics annotation takes place.
            f.lit(False).alias("hasSumstats"),
            f.lit(None).cast(t.StringType()).alias("summarystatsLocation"),
            meta_analysis_type.n_cases_per_cohort(),
            meta_analysis_type.n_samples_per_cohort(),
        ).withColumn(
            "ldPopulationStructure",
            StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
        )
        msi = MetaAnalysisStudyIndex(_df=df)
        return efo_mapping.annotate_study_index(
            study_index=msi,
            finngen_release=release.release,
        )
