"""Study Index for Finngen data source."""

from __future__ import annotations

from urllib.request import urlopen

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from gentropy.dataset.study_index import StudyIndex


class FinnGenStudyIndex:
    """Study index dataset from FinnGen.

    The following information is aggregated/extracted:

    - Study ID in the special format (e.g. FINNGEN_R10_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    finngen_phenotype_table_url: str = "https://r10.finngen.fi/api/phenos"
    finngen_release_prefix: str = "FINNGEN_R10"
    finngen_summary_stats_url_prefix: str = (
        "gs://finngen-public-data-r10/summary_stats/finngen_R10_"
    )
    finngen_summary_stats_url_suffix: str = ".gz"

    @classmethod
    def from_source(
        cls: type[FinnGenStudyIndex],
        spark: SparkSession,
    ) -> StudyIndex:
        """This function ingests study level metadata from FinnGen.

        Args:
            spark (SparkSession): Spark session object.

        Returns:
            StudyIndex: Parsed and annotated FinnGen study table.
        """
        json_data = urlopen(cls.finngen_phenotype_table_url).read().decode("utf-8")
        rdd = spark.sparkContext.parallelize([json_data])
        raw_df = spark.read.json(rdd)
        return StudyIndex(
            _df=raw_df.select(
                f.concat(f.col("phenocode")).alias("studyId"),
                f.col("phenostring").alias("traitFromSource"),
                f.col("num_cases").cast("integer").alias("nCases"),
                f.col("num_controls").cast("integer").alias("nControls"),
                (f.col("num_cases") + f.col("num_controls"))
                .cast("integer")
                .alias("nSamples"),
                f.lit(cls.finngen_release_prefix).alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.lit(True).alias("hasSumstats"),
                f.lit("377,277 (210,870 females and 166,407 males)").alias(
                    "initialSampleSize"
                ),
                f.array(
                    f.struct(
                        f.lit(377277).cast("integer").alias("sampleSize"),
                        f.lit("Finnish").alias("ancestry"),
                    )
                ).alias("discoverySamples"),
                # Cohort label is consistent with GWAS Catalog curation.
                f.array(f.lit("FinnGen")).alias("cohorts"),
                f.concat(
                    f.lit(cls.finngen_summary_stats_url_prefix),
                    f.col("phenocode"),
                    f.lit(cls.finngen_summary_stats_url_suffix),
                ).alias("summarystatsLocation"),
            ).withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=StudyIndex.get_schema(),
        )
