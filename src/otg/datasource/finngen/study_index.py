"""Study Index for Finngen data source."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FinnGenStudyIndex(StudyIndex):
    """Study index dataset from FinnGen.

    The following information is aggregated/extracted:

    - Study ID in the special format (FINNGEN_R9_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    @classmethod
    def from_source(
        cls: type[FinnGenStudyIndex],
        finngen_studies: DataFrame,
        finngen_release_prefix: str,
        finngen_summary_stats_url_prefix: str,
        finngen_summary_stats_url_suffix: str,
    ) -> FinnGenStudyIndex:
        """This function ingests study level metadata from FinnGen.

        Args:
            finngen_studies (DataFrame): FinnGen raw study table
            finngen_release_prefix (str): Release prefix pattern.
            finngen_sumstat_url_prefix (str): URL prefix for summary statistics location.
            finngen_sumstat_url_suffix (str): URL prefix suffix for summary statistics location.

        Returns:
            FinnGenStudyIndex: Parsed and annotated FinnGen study table.
        """
        return FinnGenStudyIndex(
            _df=finngen_studies.select(
                f.concat(f.lit(f"{finngen_release_prefix}_"), f.col("phenocode")).alias(
                    "studyId"
                ),
                f.col("phenostring").alias("traitFromSource"),
                f.col("num_cases").alias("nCases"),
                f.col("num_controls").alias("nControls"),
                (f.col("num_cases") + f.col("num_controls")).alias("nSamples"),
                f.lit(finngen_release_prefix).alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.lit(True).alias("hasSumstats"),
                f.lit("377,277 (210,870 females and 166,407 males)").alias(
                    "initialSampleSize"
                ),
                f.array(
                    f.struct(
                        f.lit(377277).cast("long").alias("sampleSize"),
                        f.lit("Finnish").alias("ancestry"),
                    )
                ).alias("discoverySamples"),
                f.concat(
                    f.lit(finngen_summary_stats_url_prefix),
                    f.col("phenocode"),
                    f.lit(finngen_summary_stats_url_suffix),
                ).alias("summarystatsLocation"),
            ).withColumn(
                "ldPopulationStructure",
                cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=cls.get_schema(),
        )
