"""Summary statistics ingestion for UKB PPP (EUR)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

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
