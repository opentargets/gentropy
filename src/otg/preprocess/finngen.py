"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

import sys
from urllib.request import urlopen

from pyspark.sql import SparkSession

from otg.dataset.finngen.study_index import FinnGenStudyIndex
from otg.dataset.finngen.summary_stats import FinnGenSummaryStatistics


def main(
    finngen_phenotype_table_url,
    finngen_release_prefix,
    finngen_summary_stats_url_prefix,
    finngen_summary_stats_url_suffix,
    finngen_study_index_out,
    finngen_summary_stats_out,
    spark_write_mode,
) -> None:
    """Ingest FinnGen data."""
    # Initialise Spark session.
    spark = SparkSession.builder.master("yarn").appName("ingest_finngen").getOrCreate()

    # Fetch study index.
    json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
    rdd = spark.sparkContext.parallelize([json_data])
    study_index_raw = spark.read.json(rdd)
    # Process study index.
    study_index = FinnGenStudyIndex.from_source(
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
    summary_stats = FinnGenSummaryStatistics.from_finngen_harmonized_summary_stats(
        summary_stats_raw
    ).df
    # Save summary stats.
    (
        summary_stats.sortWithinPartitions("position")
        .write.partitionBy("studyId", "chromosome")
        .mode(spark_write_mode)
        .parquet(finngen_summary_stats_out)
    )


if __name__ == "__main__":
    kwargs = dict([arg[2:].split("=") for arg in sys.argv[1:]])
    main(**kwargs)
