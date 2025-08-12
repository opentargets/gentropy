"""Interval dataset from EPIraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class IntervalsE2G:
    """Interval dataset from E2G."""

    @staticmethod
    def read(spark: SparkSession, path: str) -> DataFrame:
        """Read E2G dataset.

        Args:
            spark (SparkSession): Spark session
            path (str): Path to the dataset

        Returns:
            DataFrame: Raw, full E2G dataframe
        """
        return (
            spark.read.option("delimiter", "\t")
            .option("header", "true")
            .csv(path)
            .withColumn("file_path", f.input_file_name())
        )
    @classmethod
    def parse(
        cls: type[IntervalsE2G],
        raw_e2g_df: DataFrame,
        biosample_mapping: DataFrame,
        target_index: TargetIndex,
        biosample_index: BiosampleIndex,
    ) -> Intervals:
        """Parse E2G dataset.

        Args:
            raw_e2g_df (DataFrame): Raw E2G dataset
            biosample_mapping (DataFrame): DataFrame mapping biosample names to IDs
            target_index (TargetIndex): Target index
            biosample_index (BiosampleIndex): Biosample index

        Returns:
            Intervals: Intervals dataset
        """
        # Constant values:
        dataset_name = "E2G"
        pmid = "38014075"  # PMID for the E2G paper

        parsed_e2g_df = (
            raw_e2g_df
            .withColumn(
                "studyId",
                f.regexp_extract(f.col("file_path"), r"([^/]+)\.bed\.gz$", 1)
            )

            .withColumn("chromosome", f.regexp_replace(f.col("chr"), "^chr", ""))
            .withColumnRenamed("TargetGeneEnsemblID", "geneId")
            .withColumnRenamed("CellType", "biosampleName")
            .withColumnRenamed("Score", "score")
            .withColumnRenamed("class", "intervalType")
            .withColumn(
                "resourceScore",
                f.array(
                    f.struct(
                        f.lit("DNase").alias("name"),
                        f.col("`normalizedDNase_prom.Feature`").cast("float").alias("value"),
                    ),
                    f.struct(
                        f.lit("HiC_contacts").alias("name"),
                        f.col("`3DContact.Feature`").cast("float").alias("value"),
                    ),
                ),
            )
            .withColumn("start", f.col("start").cast("long"))
            .withColumn("end", f.col("end").cast("long"))
            .withColumn("TSS", f.col("TargetGeneTSS").cast("long"))
            .withColumn("d_start_abs", f.abs(f.col("start") - f.col("TSS")))
            .withColumn("d_end_abs",   f.abs(f.col("end")   - f.col("TSS")))
            .withColumn("nearest_edge_abs", f.least(f.col("d_start_abs"), f.col("d_end_abs")))
            .withColumn(
                "distanceToTss",
                f.when(
                    (f.col("TSS") >= f.col("start")) & (f.col("TSS") <= f.col("end")),
                    f.lit(0)
                ).otherwise(f.col("nearest_edge_abs")).cast("double")
            )
            .drop("d_start_abs", "d_end_abs", "nearest_edge_abs")
            .withColumn(
                "intervalId",
                f.sha1(f.concat_ws("_", "chromosome", "start", "end", "geneId", "studyId")),
            )
            .join(
                biosample_mapping.select("biosampleName", "biosampleId"),
                on="biosampleName",
                how="left",
            )
        )

        parsed_e2g_df = parsed_e2g_df.join(
            target_index._df.select(f.col("id").alias("geneId")),
            on="geneId",
            how="inner",
        ).join(biosample_index.df.select("biosampleId"), on="biosampleId", how="inner")

        return Intervals(
            _df=(
                parsed_e2g_df.select(
                    f.col("chromosome"),
                    f.col("start").cast("string"),
                    f.col("end").cast("string"),
                    f.col("geneId"),
                    f.col("biosampleName"),
                    f.col("intervalType"),
                    f.col("distanceToTss").cast("double"),
                    f.col("score").cast("double"),
                    f.col("resourceScore"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(pmid).alias("pmid"),
                    f.col("studyId"),
                    f.col("biosampleId"),
                    f.col("intervalId"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
