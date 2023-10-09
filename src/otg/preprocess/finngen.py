"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

import argparse
from urllib.request import urlopen

from pyspark.sql import SparkSession

from otg.dataset.finngen.study_index import FinnGenStudyIndex
from otg.dataset.finngen.summary_stats import FinnGenSummaryStatistics


def main(args) -> None:
    """Ingest FinnGen data."""
    # Initialise Spark session.
    spark = SparkSession.builder.master("yarn").appName("ingest_finngen").getOrCreate()

    # Fetch study index.
    json_data = urlopen(args.finngen_phenotype_table_url).read().decode("utf-8")
    rdd = spark.sparkContext.parallelize([json_data])
    study_index_raw = spark.read.json(rdd)
    # Process study index.
    study_index = FinnGenStudyIndex.from_source(
        study_index_raw,
        args.finngen_release_prefix,
        args.finngen_summary_stats_url_prefix,
        args.finngen_summary_stats_url_suffix,
    ).df
    # Save study index.
    (
        study_index.write.mode(args.spark_write_mode).parquet(
            args.finngen_study_index_out
        )
    )

    # Fetch summary stats.
    input_filenames = [row.summarystatsLocation for row in study_index.collect()]
    summary_stats_raw = spark.read.option("delimiter", "\t").csv(
        input_filenames, header=True
    )
    # Process summary stats.
    summary_stats = FinnGenSummaryStatistics.from_finngen_harmonized_summary_stats(
        summary_stats_raw
    ).df
    # Save summary stats.
    (
        summary_stats.sortWithinPartitions("position")
        .write.partitionBy("studyId", "chromosome")
        .mode(args.spark_write_mode)
        .parquet(args.finngen_summary_stats_out)
    )


parser = argparse.ArgumentParser(description="Process FinnGen data")
parser.add_argument(
    "--finngen_phenotype_table_url",
    help="URL to ingest FinnGen phenotype table JSON data from",
)
parser.add_argument(
    "--finngen_release_prefix", help="Prefix which will be added to all study IDs"
)
parser.add_argument(
    "--finngen_summary_stats_url_prefix",
    help="Prefix for each summary stats file URL",
)
parser.add_argument(
    "--finngen_summary_stats_url_suffix",
    help="Suffix for each summary stats file URL",
)
parser.add_argument(
    "--finngen_study_index_out",
    help="Output URL in Google Storage to save the study index",
)
parser.add_argument(
    "--finngen_summary_stats_out",
    help="Output URL in Google Storage to save the summary stats",
)
parser.add_argument(
    "--spark_write_mode",
    help="Spark write mode which is applied to both study index and summary stats",
)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
