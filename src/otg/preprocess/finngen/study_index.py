"""FinnGen ingestion: study index."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from otg.common.schemas import parse_spark_schema
from otg.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@dataclass
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
    def get_schema(cls: type[FinnGenStudyIndex]) -> StructType:
        """Provides the schema for the FinnGenStudyIndex dataset."""
        return parse_spark_schema("studies.json")

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
            finngen_summary_stats_df_url_prefix (str): URL prefix for summary statistics location.
            finngen_summary_stats_df_url_suffix (str): URL prefix suffix for summary statistics location.

        Returns:
            FinnGenStudyIndex: Parsed and annotated FinnGen study table.
        """
        return cls(
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
                f.concat(
                    f.lit(finngen_summary_stats_url_prefix),
                    f.col("phenocode"),
                    f.lit(finngen_summary_stats_url_suffix),
                ).alias("summarystatsLocation"),
            ),
            _schema=cls.get_schema(),
        )


def ingest_finngen_study_index(
    finngen_phenotype_table_url,
    finngen_release_prefix,
    finngen_summary_stats_url_prefix,
    finngen_summary_stats_url_suffix,
    finngen_study_index_out,
    spark_write_mode,
):
    """Study index ingestion for FinnGen.

    Args:
        finngen_phenotype_table_url (str): FinnGen API for fetching the list of studies.
        finngen_release_prefix (str): Release prefix pattern.
        finngen_summary_stats_url_prefix (str): URL prefix for summary statistics location.
        finngen_summary_stats_url_suffix (str): URL prefix suffix for summary statistics location.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        spark_write_mode (str): Dataframe write mode.
    """
    spark = SparkSession.builder.master("yarn").appName("ingest_finngen").getOrCreate()

    # Read the JSON data from the URL.
    json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
    rdd = spark.sparkContext.parallelize([json_data])
    df = spark.read.json(rdd)

    # Parse the study index data.
    finngen_study_index = FinnGenStudyIndex.from_source(
        df,
        finngen_release_prefix,
        finngen_summary_stats_url_prefix,
        finngen_summary_stats_url_suffix,
    )

    # Write the study index output.
    finngen_study_index.df.write.mode(spark_write_mode).parquet(finngen_study_index_out)
    logging.info(
        f"Number of studies in the study index: {finngen_study_index.df.count()}"
    )


if __name__ == "__main__":
    ingest_finngen_study_index(*sys.argv[1:])
