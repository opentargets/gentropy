"""Summary statistics ingestion for UKB PPP (EUR)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from gentropy import Session
from gentropy.common.processing import harmonise_summary_stats
from gentropy.dataset.summary_statistics import SummaryStatistics


@dataclass
class UkbPppEurSummaryStats:
    """Summary statistics dataset for UKB PPP (EUR)."""

    @classmethod
    def from_source(
        cls: type[UkbPppEurSummaryStats],
        spark: SparkSession,
        raw_summary_stats_path: str,
        tmp_variant_annotation_path: str,
        chromosome: str,
        study_index_path: str,
    ) -> SummaryStatistics:
        """Ingest and harmonise all summary stats for UKB PPP (EUR) data.

        Args:
            spark (SparkSession): Spark session object.
            raw_summary_stats_path (str): Input raw summary stats path.
            tmp_variant_annotation_path (str): Input variant annotation dataset path.
            chromosome (str): Which chromosome to process.
            study_index_path (str): The path to study index, which is necessary in some cases to populate the sample size column.

        Returns:
            SummaryStatistics: Processed summary statistics dataset for a given chromosome.
        """
        df = harmonise_summary_stats(
            spark,
            raw_summary_stats_path,
            tmp_variant_annotation_path,
            chromosome,
            colname_position="GENPOS",
            colname_allele0="ALLELE0",
            colname_allele1="ALLELE1",
            colname_a1freq="A1FREQ",
            colname_info="INFO",
            colname_beta="BETA",
            colname_se="SE",
            colname_mlog10p="LOG10P",
            colname_n="N",
        )

        # Create the summary statistics object.
        return SummaryStatistics(
            _df=df,
            _schema=SummaryStatistics.get_schema(),
        )

    @classmethod
    def process_summary_stats_per_chromosome(
        cls,
        session: Session,
        raw_summary_stats_path: str,
        tmp_variant_annotation_path: str,
        summary_stats_output_path: str,
        study_index_path: str,
    ) -> None:
        """Processes summary statistics for each chromosome, partitioning and writing results.

        Args:
            session (Session): The Gentropy session session to use for distributed data processing.
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
                cls.from_source(
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
