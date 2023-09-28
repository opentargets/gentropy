"""Datasource ingestion: FinnGen."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

from otg.common.schemas import parse_spark_schema
from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@dataclass
class FinnGenSummaryStats(SummaryStatistics):
    """Summary statistics dataset for FinnGen."""

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the FinnGenSummaryStats dataset."""
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
            .filter(f.col("pos").cast(t.IntegerType()).isNotNull()).select(
                # Add study idenfitier.
                f.lit(study_id).cast(t.StringType()).alias("studyId"),
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


def ingest_finngen_summary_stats(
    finngen_study_id,
    finngen_summary_stats_location,
    finngen_summary_stats_out,
    spark_write_mode,
):
    """Summary stats ingestion for FinnGen.

    Args:
        finngen_study_id (str): ID of the individual study being ingested.
        finngen_summary_stats_location (str): Google Storage URI for the summary stats to ingest.
        finngen_summary_stats_out (str): Output path for the FinnGen summary statistics dataset.
        spark_write_mode (str): Dataframe write mode.
    """
    spark = SparkSession.builder.master("yarn").appName("ingest_finngen").getOrCreate()

    # Ingest the data.
    logging.info(
        f"Processing {finngen_study_id} with summary statistics in {finngen_summary_stats_location}"
    )
    summary_stats_df = (
        spark.read.option("delimiter", "\t")
        .csv(finngen_summary_stats_location, header=True)
        .repartition("#chrom")
    )

    # Process and output the data.
    out_filename = f"{finngen_summary_stats_out}"
    FinnGenSummaryStats.from_finngen_harmonized_summary_stats(
        summary_stats_df, finngen_study_id
    ).df.sortWithinPartitions("position").write.partitionBy("chromosome").mode(
        spark_write_mode
    ).parquet(
        out_filename
    )


if __name__ == "__main__":
    ingest_finngen_summary_stats(*sys.argv[1:])
