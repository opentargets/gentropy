"""Summary statistics ingestion for FinnGen."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.types import StringType, StructField, StructType

from otg.common.session import Session
from otg.common.utils import calculate_confidence_interval, parse_pvalue
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class FinnGenSummaryStats(SummaryStatistics):
    """Summary statistics dataset for FinnGen."""

    raw_schema: t.StructType = StructType(
        [
            StructField("#chrom", StringType(), True),
            StructField("pos", StringType(), True),
            StructField("ref", StringType(), True),
            StructField("alt", StringType(), True),
            StructField("rsids", StringType(), True),
            StructField("nearest_genes", StringType(), True),
            StructField("pval", StringType(), True),
            StructField("mlogp", StringType(), True),
            StructField("beta", StringType(), True),
            StructField("sebeta", StringType(), True),
            StructField("af_alt", StringType(), True),
            StructField("af_alt_cases", StringType(), True),
            StructField("af_alt_controls", StringType(), True),
        ]
    )

    @classmethod
    def from_source(
        cls: type[FinnGenSummaryStats],
        session: Session,
        raw_files: str,
    ) -> FinnGenSummaryStats:
        """Ingests all summary statst for all FinnGen studies.

        Args:
            session (Session): Session object.
            raw_files (str): Path to the raw summary statistics file.

        Returns:
            FinnGenSummaryStats: Processed summary statistics dataset
        """
        processed_summary_stats_df = (
            session.spark.readStream.format("csv")
            .schema(cls.raw_schema)
            .option("delimiter", "\t")
            .option("header", True)
            .load(raw_files)
            # session.spark.read.schema(cls.raw_schema)
            # .option("delimiter", "\t")
            # .csv(raw_files, header=True)
            # Drop rows which don't have proper position.
            .filter(f.col("pos").cast(t.IntegerType()).isNotNull())
            .select(
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
