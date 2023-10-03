"""Datasource ingestion: FinnGen."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@dataclass
class FinnGenSummaryStats(SummaryStatistics):
    """Summary statistics dataset for FinnGen."""

    @classmethod
    def from_finngen_harmonized_summary_stats(
        cls: type[FinnGenSummaryStats],
        summary_stats_df: DataFrame,
        study_id: str,
    ) -> FinnGenSummaryStats:
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
