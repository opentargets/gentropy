"""Study Index for Finngen data source."""
from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from gentropy.dataset.study_index import StudyIndex


class FinngenUkbMetaStudyIndex(StudyIndex):
    """Study index dataset from FinnGen UKB meta-analysis."""

    @classmethod
    def from_source(
        cls: type[FinngenUkbMetaStudyIndex],
        spark: SparkSession,
        raw_study_index_path: str,
    ) -> StudyIndex:
        """This function ingests study level metadata from FinnGen UKB meta-analysis.

        Args:
            spark (SparkSession): Spark session object.
            raw_study_index_path (str): Raw study index path.

        Returns:
            StudyIndex: Parsed and annotated FinnGen UKB meta-analysis study table.
        """
        # Read the raw study index and process.
        study_index_df = (
            spark.read.csv(raw_study_index_path, sep="\t", header=True)
            .select(
                f.lit("gwas").alias("studyType"),
                f.lit("FINNGEN_R11_UKB_META").alias("projectId"),
                f.col("_gentropy_study_id").alias("studyId"),
                f.col("name").alias("traitFromSource"),
                f.lit(True).alias("hasSumstats"),
                f.col("_gentropy_summary_stats_link").alias("summarystatsLocation"),
                (f.col("fg_n_cases") + f.col("ukbb_n_cases") + f.col("fg_n_controls") + f.col("ukbb_n_controls")).alias("nSamples")
            )
        )
        # Add population structure.
        study_index_df = (
            study_index_df
            .withColumn(
                "discoverySamples",
                f.array(
                    f.struct(
                        f.col("nSamples").cast("integer").alias("sampleSize"),
                        f.lit("European").alias("ancestry"),
                    )
                )
            )
            .withColumn(
                "ldPopulationStructure",
                cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
            )
        )

        return StudyIndex(
            _df=study_index_df,
            _schema=StudyIndex.get_schema(),
        )
