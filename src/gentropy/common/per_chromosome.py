"""Spark operations to make efficient per-chromosome processing possible."""

from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, SparkSession

from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.finngen_ukb_meta.summary_stats import (
    FinngenUkbMetaSummaryStats,
)
from gentropy.datasource.ukb_ppp_eur.summary_stats import UkbPppEurSummaryStats


class VariantFlipper:
    def __init__(self, vi: VariantIndex):
        self.vi = vi

    @property
    def flipped(self) -> Column:
        """Get the flipped variant."""
        return f.concat_ws(
            "_",
            f.col("chromosome"),
            f.col("position"),
            f.col("alternateAllele"),
            f.col("referenceAllele"),
        ).alias("flipped")

    @property
    def direct(self) -> Column:
        """Get the direct variant."""
        return f.concat_ws(
            "_",
            f.col("chromosome"),
            f.col("position"),
            f.col("referenceAllele"),
            f.col("alternateAllele"),
        ).alias("flipped")

    def variant_direction(self) -> DataFrame:
        """Get the variant direction dataset."""
        return self.vi.df.select(f.col("position"), f.struct(self.direct, self.flipped))


def process_summary_stats_per_chromosome(
    session: SparkSession,
    ingestion_class: type[UkbPppEurSummaryStats] | type[FinngenUkbMetaSummaryStats],
    raw_summary_stats_path: str,
    tmp_variant_annotation_path: str,
    summary_stats_output_path: str,
    study_index_path: str,
) -> None:
    """Processes summary statistics for each chromosome, partitioning and writing results.

    Args:
        session (SparkSession): The Spark session to use for distributed data processing.
        ingestion_class (type[UkbPppEurSummaryStats] | type[FinngenUkbMetaSummaryStats]): The class used to handle ingestion of source data. Must have a `from_source` method returning a DataFrame.
        raw_summary_stats_path (str): The path to the raw summary statistics files.
        tmp_variant_annotation_path (str): The path to temporary variant annotation data, used for chromosome joins.
        summary_stats_output_path (str): The output path to write processed summary statistics as parquet files.
        study_index_path (str): The path to study index, which is necessary in some cases to populate the sample size column.
    """
    # Set mode to overwrite for processing the first chromosome.
    write_mode = "overwrite"
    # Chromosome 23 is X, this is handled downstream.
    for chromosome in list(range(1, 24)):
        logging_message = f"  Processing chromosome {chromosome}"
        session.logger.info(logging_message)
        (
            ingestion_class.from_source(
                spark=session.spark,
                raw_summary_stats_path=raw_summary_stats_path,
                tmp_variant_annotation_path=tmp_variant_annotation_path,
                chromosome=str(chromosome),
                study_index_path=study_index_path,
            )
            .df.coalesce(1)
            .repartition("studyId", "chromosome")
            .write.partitionBy("studyId", "chromosome")
            .mode(write_mode)
            .parquet(summary_stats_output_path)
        )
        # Now that we have written the first chromosome, change mode to append for subsequent operations.
        write_mode = "append"
