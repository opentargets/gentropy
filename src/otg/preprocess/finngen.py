"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.request import urlopen

import click
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.dataset.study_index import StudyIndex
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def ingest_finngen_study_index(
    finngen_studies: DataFrame,
    finngen_release_prefix: str,
    finngen_summary_stats_url_prefix: str,
    finngen_summary_stats_url_suffix: str,
) -> StudyIndex:
    """Ingest study level metadata (study index) from FinnGen."""
    df = finngen_studies.select(
        f.concat(f.lit(f"{finngen_release_prefix}_"), f.col("phenocode")).alias(
            "studyId"
        ),
        f.col("phenostring").alias("traitFromSource"),
        f.col("num_controls").alias("nControls"),
        (f.col("num_cases") + f.col("num_controls")).alias("nSamples"),
        f.lit(finngen_release_prefix).alias("projectId"),
        f.lit("gwas").alias("studyType"),
        f.lit(True).alias("hasSumstats"),
        f.lit("377,277 (210,870 females and 166,407 males)").alias("initialSampleSize"),
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
        StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
    )
    return StudyIndex(_df=df, _schema=StudyIndex.get_schema())


def ingest_finngen_summary_stats(summary_stats_df: DataFrame) -> SummaryStatistics:
    """Ingest FinnGen summary statistics for all studies."""
    df = (
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
    return StudyIndex(_df=df, _schema=StudyIndex.get_schema())


def main(
    finngen_phenotype_table_url,
    finngen_release_prefix,
    finngen_summary_stats_url_prefix,
    finngen_summary_stats_url_suffix,
    finngen_study_index_out,
    finngen_summary_stats_out,
    spark_write_mode,
) -> None:
    """Run FinnGen ingestion step."""
    # Initialise Spark session.
    spark = SparkSession.builder.master("yarn").appName("ingest_finngen").getOrCreate()

    # Fetch study index.
    json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
    rdd = spark.sparkContext.parallelize([json_data])
    study_index_raw = spark.read.json(rdd)
    # Process study index.
    study_index = ingest_finngen_study_index(
        study_index_raw,
        finngen_release_prefix,
        finngen_summary_stats_url_prefix,
        finngen_summary_stats_url_suffix,
    ).df
    # Save study index.
    (study_index.write.mode(spark_write_mode).parquet(finngen_study_index_out))

    # Fetch summary stats.
    input_filenames = [row.summarystatsLocation for row in study_index.collect()]
    summary_stats_raw = spark.read.option("delimiter", "\t").csv(
        input_filenames, header=True
    )
    # Process summary stats.
    summary_stats = ingest_finngen_summary_stats(summary_stats_raw).df
    # Save summary stats.
    (
        summary_stats.sortWithinPartitions("position")
        .write.partitionBy("studyId", "chromosome")
        .mode(spark_write_mode)
        .parquet(finngen_summary_stats_out)
    )


@click.command()
@click.option(
    "--finngen_phenotype_table_url",
    help="URL to ingest FinnGen phenotype table JSON data from",
)
@click.option(
    "--finngen_release_prefix", help="Prefix which will be added to all study IDs"
)
@click.option(
    "--finngen_summary_stats_url_prefix",
    help="Prefix for each summary stats file URL",
)
@click.option(
    "--finngen_summary_stats_url_suffix",
    help="Suffix for each summary stats file URL",
)
@click.option(
    "--finngen_study_index_out",
    help="Output URL in Google Storage to save the study index",
)
@click.option(
    "--finngen_summary_stats_out",
    help="Output URL in Google Storage to save the summary stats",
)
@click.option(
    "--spark_write_mode",
    help="Spark write mode which is applied to both study index and summary stats",
)
def main_cli(*args, **kwargs) -> None:
    """Wrapped CLI version of the main function."""
    main(*args, **kwargs)


if __name__ == "__main__":
    main_cli()
