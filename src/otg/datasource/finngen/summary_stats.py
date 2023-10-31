"""Summary statistics ingestion for FinnGen."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class FinnGenSummaryStats(SummaryStatistics):
    """Summary statistics dataset for FinnGen."""

    @classmethod
    def from_finngen_harmonized_summary_stats(
        cls: type[FinnGenSummaryStats],
        summary_stats_df: DataFrame,
    ) -> FinnGenSummaryStats:
        """Ingests all summary statst for all FinnGen studies.

        Args:
            summary_stats_df (DataFrame): Raw summary statistics dataframe

        Returns:
            FinnGenSummaryStats: Processed summary statistics dataset
        """
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
