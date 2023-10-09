"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.session import Session
from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.config import FinnGenStepConfig
from otg.dataset.study_index import StudyIndex
from otg.dataset.summary_statistics import SummaryStatistics

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


@dataclass
class FinnGenSummaryStats(SummaryStatistics):
    """Summary statistics dataset for FinnGen."""

    @classmethod
    def from_finngen_harmonized_summary_stats(
        cls: type[FinnGenSummaryStats],
        summary_stats_df: DataFrame,
    ) -> FinnGenSummaryStats:
        """Ingests all summary statst for all FinnGen studies."""
        processed_summary_stats_df = (
            summary_stats_df
            # Drop rows which don't have proper position.
            .filter(f.col("pos").cast(t.IntegerType()).isNotNull()).select(
                # From the full path, extracts just the filename, and converts to upper case to get the study ID.
                f.upper(f.regexp_extract(f.input_file_name(), r"([^/]+)\.gz", 1)).alias(
                    "studyId"
                ),
                # Add variant information.
                f.concat_ws(
                    "_",
                    f.col("#chrom"),
                    f.col("pos"),
                    f.col("ref"),
                    f.col("alt"),
                ).alias("variantId"),
                f.col("#chrom").alias("chromosome"),
                f.col("pos").cast(t.IntegerType()).alias("position"),
                # Parse p-value into mantissa and exponent.
                *parse_pvalue(f.col("pval")),
                # Add beta, standard error, and allele frequency information.
                f.col("beta").cast("double"),
                f.col("sebeta").cast("double").alias("standardError"),
                f.col("af_alt").cast("float").alias("effectAlleleFrequencyFromSource"),
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
        )

        # Initializing summary statistics object:
        return cls(
            _df=processed_summary_stats_df,
            _schema=cls.get_schema(),
        )


@dataclass
class FinnGenStep(FinnGenStepConfig):
    """FinnGen ingestion step."""

    session: Session = Session()

    def run(self: FinnGenStep) -> None:
        """Run FinnGen ingestion step."""
        # Read the JSON data from the URL.
        json_data = urlopen(self.finngen_phenotype_table_url).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([json_data])
        df = self.session.spark.read.json(rdd)

        # Parse the study index data.
        finngen_studies = FinnGenStudyIndex.from_source(
            df,
            self.finngen_release_prefix,
            self.finngen_sumstat_url_prefix,
            self.finngen_sumstat_url_suffix,
        )

        # Write the study index output.
        finngen_studies.df.write.mode(self.session.write_mode).parquet(
            self.finngen_study_index_out
        )

        # Prepare list of files for ingestion.
        input_filenames = [
            row.summarystatsLocation for row in finngen_studies.collect()
        ]
        summary_stats_df = self.session.spark.read.option("delimiter", "\t").csv(
            input_filenames, header=True
        )

        # Specify data processing instructions.
        summary_stats_df = FinnGenSummaryStats.from_finngen_harmonized_summary_stats(
            summary_stats_df
        ).df

        # Sort and partition for output.
        summary_stats_df.sortWithinPartitions("position").write.partitionBy(
            "studyId", "chromosome"
        ).mode(self.session.write_mode).parquet(self.finngen_summary_stats_out)
