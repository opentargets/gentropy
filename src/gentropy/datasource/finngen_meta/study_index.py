"""Finngen meta analysis study index data source module."""

from __future__ import annotations

from pyspark.sql import functions as f

from gentropy import StudyIndex
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex
from gentropy.datasource.finngen_meta import (
    EFOCuration,
    FinngenMetaManifest,
)


class FinngenMetaStudyIndex:
    """FinnGen meta-analysis study index."""

    @classmethod
    def from_finngen_manifest(
        cls: type[FinngenMetaStudyIndex],
        manifest: FinngenMetaManifest,
        efo_curation: EFOCuration,
    ) -> StudyIndex:
        """Create the FinnGen meta-analysis study index from the manifest."""
        # 1. Read the mapping

        df = manifest.df.select(
            f.lit("gwas").alias("studyType"),
            f.lit(manifest.meta.value).alias("projectId"),
            f.concat_ws(
                "_",
                f.lit(manifest.meta.value),
                f.col("studyPhenotype"),
            ).alias("studyId"),
            f.col("traitFromSource"),
            f.col("hasSumstats"),
            f.col("summarystatsLocation"),
            f.col("discoverySamples"),
            f.col("nSamples"),
        )

        # Add population structure.
        study_index_df = df.withColumn(
            "ldPopulationStructure",
            StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
        )

        # Create study index.
        study_index = StudyIndex(_df=study_index_df)

        # Add EFO mappings.
        study_index = FinnGenStudyIndex.join_efo_mapping(
            study_index, efo_curation.df, finngen_release="R12"
        )
        # Coalesce to a single file.
        return StudyIndex(_df=study_index.df.coalesce(1))
