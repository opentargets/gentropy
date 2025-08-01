"""Summary statistics ingestion for FinnGen UKB meta-analysis."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from gentropy.common.processing import harmonise_summary_stats
from gentropy.dataset.summary_statistics import SummaryStatistics


@dataclass
class FinngenUkbMetaSummaryStats:
    """Summary statistics dataset for FinnGen UKB meta-analysis."""

    @classmethod
    def from_source(
        cls: type[FinngenUkbMetaSummaryStats],
        spark: SparkSession,
        raw_summary_stats_path: str,
        tmp_variant_annotation_path: str,
        chromosome: str,
        study_index_path: str,
    ) -> SummaryStatistics:
        """Ingest and harmonise all summary stats for FinnGen UKB meta-analysis data.

        Args:
            spark (SparkSession): Spark session object.
            raw_summary_stats_path (str): Input raw summary stats path.
            tmp_variant_annotation_path (str): Input variant annotation dataset path.
            chromosome (str): Which chromosome to process.
            study_index_path (str): The path to study index, which is necessary in some cases to populate the sample size column.

        Returns:
            SummaryStatistics: Processed summary statistics dataset for a given chromosome.
        """
        # Run the harmonisation steps.
        df = harmonise_summary_stats(
            spark,
            raw_summary_stats_path,
            tmp_variant_annotation_path,
            chromosome,
            colname_position="POS",
            colname_allele0="REF",
            colname_allele1="ALT",
            colname_a1freq=None,
            colname_info=None,
            colname_beta="all_inv_var_meta_beta",
            colname_se="all_inv_var_meta_sebeta",
            colname_mlog10p="all_inv_var_meta_mlogp",
            colname_n=None,
        )

        # Populate the sample size column from the study index.
        study_index = spark.read.parquet(study_index_path).select(
            "studyId",
            f.col("nSamples").cast("integer").alias("sampleSize")
        )
        df = df.join(study_index, on=["studyId"], how="inner")

        # Create the summary statistics object.
        return SummaryStatistics(
            _df=df,
            _schema=SummaryStatistics.get_schema(),
        )
