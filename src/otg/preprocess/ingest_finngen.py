"""Datasource ingestion: FinnGen."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

from otg.common.schemas import parse_spark_schema
from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.dataset.study_index import StudyIndex
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class StudyIndexFinnGen(StudyIndex):
    """Study index dataset from FinnGen.

    The following information is aggregated/extracted:

    - Study ID in the special format (FINNGEN_R9_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    @classmethod
    def get_schema(cls: type[StudyIndexFinnGen]) -> StructType:
        """Provides the schema for the StudyIndexFinnGen dataset."""
        return parse_spark_schema("studies.json")

    @classmethod
    def from_source(
        cls: type[StudyIndexFinnGen],
        finngen_studies: DataFrame,
        finngen_release_prefix: str,
        finngen_summary_stats_url_prefix: str,
        finngen_summary_stats_url_suffix: str,
    ) -> StudyIndexFinnGen:
        """This function ingests study level metadata from FinnGen.

        Args:
            finngen_studies (DataFrame): FinnGen raw study table
            finngen_release_prefix (str): Release prefix pattern.
            finngen_summary_stats_df_url_prefix (str): URL prefix for summary statistics location.
            finngen_summary_stats_df_url_suffix (str): URL prefix suffix for summary statistics location.

        Returns:
            StudyIndexFinnGen: Parsed and annotated FinnGen study table.
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


@dataclass
class SummaryStatisticsFinnGen(SummaryStatistics):
    """Summary statistics dataset for FinnGen."""

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatisticsFinnGen dataset."""
        return parse_spark_schema("summary_statistics.json")

    @classmethod
    def from_finngen_harmonized_summary_stats(
        cls: type[SummaryStatistics],
        summary_stats_df: DataFrame,
        study_id: str,
    ) -> SummaryStatistics:
        """Summary statistics ingestion for one FinnGen study."""
        processed_summary_stats_df = (
            summary_stats_df
            # Drop rows which don't have proper position.
            .filter(f.col("pos").cast(t.IntegerType()).isNotNull())
            .select(
                # Add study idenfitier.
                f.lit(study_id).cast(t.StringType()).alias("studyId"),
                # Add variant information.
                f.concat_ws(
                    "_",
                    f.col("chrom"),
                    f.col("pos"),
                    f.col("ref"),
                    f.col("alt"),
                ).alias("variantId"),
                f.col("chrom").alias("chromosome"),
                f.col("pos").cast(t.IntegerType()).alias("position"),
                # Parse p-value into mantissa and exponent.
                *parse_pvalue(f.col("p_value")),
                # Add beta, standard error, and allele frequency information.
                f.col("beta"),
                f.col("sebeta").alias("standardError"),
                f.col("af_alt").alias("effectAlleleFrequencyFromSource"),
            )
            # Calculating the confidence intervals.
            .select(
                "*",
                *calculate_confidence_interval(
                    f.col("pValueMantissa"),
                    f.col("pValueExponent"),
                    f.col("beta"),
                    f.col("standardError"),
                ),
            )
            .repartition(1)
            .sortWithinPartitions("chromosome", "position")
        )

        # Initializing summary statistics object:
        return cls(
            _df=processed_summary_stats_df,
            _schema=cls.get_schema(),
        )


def ingest_finngen(
    finngen_phenotype_table_url,
    finngen_release_prefix,
    finngen_summary_stats_url_prefix,
    finngen_summary_stats_url_suffix,
    finngen_study_index_out,
    finngen_summary_stats_out,
    spark_write_mode,
):
    """Datasource ingestion: FinnGen.

    Args:
        finngen_phenotype_table_url (str): FinnGen API for fetching the list of studies.
        finngen_release_prefix (str): Release prefix pattern.
        finngen_summary_stats_url_prefix (str): URL prefix for summary statistics location.
        finngen_summary_stats_url_suffix (str): URL prefix suffix for summary statistics location.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        finngen_summary_stats_out (str): Output path for the FinnGen summary statistics dataset.
        spark_write_mode (str): Dataframe write mode.
    """
    spark = SparkSession.builder.appName("ingest_finngen").getOrCreate()

    # Read the JSON data from the URL.
    json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
    rdd = spark.sparkContext.parallelize([json_data])
    df = spark.read.json(rdd)

    # Parse the study index data.
    finngen_study_index = StudyIndexFinnGen.from_source(
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

    # Process the summary statistics for each study in the study index.
    for row in finngen_study_index.df.select(
        "studyId", "summarystatsLocation"
    ).collect():
        logging.info(
            f"Processing {row.studyId} with summary statistics in {row.summarystatsLocation}"
        )
        summary_stats_df = spark.read.option("delimiter", "\t").csv(
            row.summarystatsLocation, header=True
        )
        out_filename = f"{finngen_summary_stats_out}/{row.studyId}.parquet"
        SummaryStatisticsFinnGen.from_finngen_harmonized_summary_stats(
            summary_stats_df, row.studyId
        ).df.write.mode(spark_write_mode).parquet(out_filename)


if __name__ == "__main__":
    ingest_finngen(*sys.argv[1:])
