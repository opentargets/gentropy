"""Step to run UKB PPP (EUR) data ingestion."""

from __future__ import annotations

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.datasource.ukb_ppp_eur.study_index import UkbPppEurStudyIndex
from gentropy.datasource.ukb_ppp_eur.summary_stats import UkbPppEurSummaryStats


class UkbPppEurStep:
    """UKB PPP (EUR) data ingestion and harmonisation."""

    def __init__(
        self, session: Session, raw_study_index_path: str, raw_summary_stats_path: str, variant_annotation_path: str, tmp_variant_annotation_path: str, output_path: str
    ) -> None:
        """Run UKB PPP (EUR) data ingestion and harmonisation step.

        Args:
            session (Session): Session object.
            raw_study_index_path (str): Input raw study index path.
            raw_summary_stats_path (str): Input raw summary stats path.
            variant_annotation_path (str): Input variant annotation dataset path.
            tmp_variant_annotation_path (str): Temporary output path for variant annotation dataset.
            output_path (str): Output path.
        """
        session.logger.info("Pre-compute the direct and flipped variant annotation dataset.")
        va_df = (
            session
            .spark
            .read
            .parquet(variant_annotation_path)
        )
        va_df_direct = (
            va_df.
            select(
                f.col("chromosome").alias("vaChromosome"),
                f.col("variantId"),
                f.concat(
                    f.col("chromosome"),
                    f.lit("_"),
                    f.col("position"),
                    f.lit("_"),
                    f.col("referenceAllele"),
                    f.lit("_"),
                    f.col("alternateAllele")
                ).alias("ukb_ppp_id"),
                f.lit("direct").alias("direction")
            )
        )
        va_df_flip = (
            va_df.
            select(
                f.col("chromosome").alias("vaChromosome"),
                f.col("variantId"),
                f.concat(
                    f.col("chromosome"),
                    f.lit("_"),
                    f.col("position"),
                    f.lit("_"),
                    f.col("alternateAllele"),
                    f.lit("_"),
                    f.col("referenceAllele")
                ).alias("ukb_ppp_id"),
                f.lit("flip").alias("direction")
            )
        )
        (
            va_df_direct.union(va_df_flip)
            .coalesce(1)
            .repartition("vaChromosome")
            .write
            .partitionBy("vaChromosome")
            .mode("overwrite")
            .parquet(tmp_variant_annotation_path)
        )

        session.logger.info("Process study index.")
        (
            UkbPppEurStudyIndex.from_source(
                spark=session.spark,
                raw_study_index_path=raw_study_index_path,
                raw_summary_stats_path=raw_summary_stats_path,
            )
            .df
            .write
            .mode("overwrite")
            .parquet(f"{output_path}/study_index")
        )

        session.logger.info("Process and harmonise summary stats.")
        # Set mode to overwrite for processing the first chromosome.
        write_mode = "overwrite"
        # Chromosome 23 is X, this is handled downstream.
        for chromosome in list(range(1, 24)):
            logging_message = f"  Processing chromosome {chromosome}"
            session.logger.info(logging_message)
            (
                UkbPppEurSummaryStats.from_source(
                    spark=session.spark,
                    raw_summary_stats_path=raw_summary_stats_path,
                    tmp_variant_annotation_path=tmp_variant_annotation_path,
                    chromosome=str(chromosome),
                )
                .df
                .coalesce(1)
                .repartition("studyId", "chromosome")
                .write
                .partitionBy("studyId", "chromosome")
                .mode(write_mode)
                .parquet(f"{output_path}/summary_stats")
            )
            # Now that we have written the first chromosome, change mode to append for subsequent operations.
            write_mode = "append"
