"""Finngen meta analysis study index data source module."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f

from gentropy import StudyIndex
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import (
    FinnGenMetaManifest,
    MetaAnalysisDataSource,
)


class FinnGenMetaStudyIndex:
    """FinnGen meta-analysis study index."""

    @classmethod
    def get_constants(cls) -> dict[str, dict[str, Column]]:
        """Get constants for FinnGen meta-analysis study index.

        Returns:
            dict[str, dict[str, Column]]: Constants for each meta-analysis data source.
        """
        return {
            MetaAnalysisDataSource.FINNGEN_UKBB.value: {
                "initialSampleSize": f.lit(
                    "920,880 (FinnGenR12: nNFE=500,349; pan-UKBB-EUR: nEUR=420,531)"
                ),  # based on https://metaresults-ukbb.finngen.fi/about
                "cohorts": f.array(f.lit("FinnGen"), f.lit("pan-UKBB-EUR")),
                "publicationDate": f.lit("2024-11-01"),
            },
            MetaAnalysisDataSource.FINNGEN_UKBB_MVP.value: {
                "initialSampleSize": f.lit(
                    "1,550,147 (MVP: nEUR=449,042, nAFR=121,177, nAMR=59,048; FinnGenR12: nNFE=500,349; pan-UKBB-EUR: nEUR=420,531)"
                ),  # based on https://mvp-ukbb.finngen.fi/about
                "publicationDate": f.lit("2024-11-01"),
                "cohorts": f.array(
                    f.lit("MVP"), f.lit("FinnGen"), f.lit("pan-UKBB-EUR")
                ),
            },
        }

    @classmethod
    def from_finngen_manifest(
        cls: type[FinnGenMetaStudyIndex],
        manifest: FinnGenMetaManifest,
        efo_mapping: EFOMapping,
    ) -> StudyIndex:
        """Create the FinnGen meta-analysis study index from the manifest.

        Args:
            manifest (FinnGenMetaManifest): FinnGen meta-analysis manifest.
            efo_mapping (EFOMapping): EFO mapping data source.

        Returns:
            StudyIndex: FinnGen meta-analysis study index.
        """
        # Read the mapping
        df = manifest.df.select(
            f.col("studyId"),
            f.col("projectId"),
            f.lit("gwas").alias("studyType"),
            f.col("traitFromSource"),
            f.col("hasSumstats"),
            f.col("summarystatsLocation"),
            f.col("discoverySamples"),
            f.col("nSamples"),
            f.col("nCases"),
            f.col("nControls"),
            # Add constant columns
            *[
                value.alias(key)
                for key, value in cls.get_constants()[manifest.meta.value].items()
            ],
            # Compute the ld structure `ldPopulationStructure` from discovery samples.
            StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")).alias(
                "ldPopulationStructure"
            ),
        )

        # Create study index.
        study_index = StudyIndex(_df=df)

        # Add EFO mappings - `traitFromSourceMappedIds`.
        study_index = efo_mapping.annotate_study_index(
            study_index, finngen_release="R12"
        )

        # Coalesce to a single file.
        return StudyIndex(
            _df=study_index.df.coalesce(1),
            _schema=StudyIndex.get_schema(),
        )
