"""Interval dataset from EPIraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

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
        target_index: TargetIndex,
    ) -> Intervals:
        """Parse E2G dataset.

        Args:
            raw_e2g_df (DataFrame): Raw E2G dataset
            target_index (TargetIndex): Target index

        Returns:
            Intervals: Intervals dataset
        """
        # Constant values:
        dataset_name = "epiraction"
        pmid = "40027634"  # PMID for the EPIraction paper

        # Read the e2g file:
        parsed_e2g_df = (
            raw_e2g_df.withColumn(
                "studyId", f.regexp_extract(f.col("file_path"), r"([^/]+)\.bed\.gz$", 1)
            )
            .withColumn("chromosome", f.regexp_replace("#chr", "^chr", ""))
            .withColumnRenamed("TargetGeneEnsemblID", "geneId")
            .withColumnRenamed("CellType", "biosampleName")
            .withColumnRenamed("Score", "score")
            .withColumnRenamed("class", "intervalType")
            .withColumnRenamed("TargetGeneTSS", "distanceToTSS")
            .withColumn(
                "resourceScore",
                f.array(
                    f.struct(
                        f.lit("DNase").alias("name"),
                        f.col("`normalizedDNase_prom.Feature`")
                        .cast("float")
                        .alias("value"),
                    ),
                    f.struct(
                        f.lit("HiC_contacts").alias("name"),
                        f.col("`3DContact.Feature`").cast("float").alias("value"),
                    ),
                ),
            )
        )

        parsed_e2g_df = parsed_e2g_df.join(
            target_index._df.select(f.col("id").alias("geneId")),
            on="geneId",
            how="inner",
        )

        dataset_name = "E2G"
        pmid = "38014075"  # PMID for the EPIraction paper

        return Intervals(
            _df=(
                parsed_e2g_df.select(
                    f.col("chromosome"),
                    f.col("start").cast("string"),
                    f.col("end").cast("string"),
                    f.col("geneId"),
                    f.col("biosampleName"),
                    f.col("intervalType"),
                    f.col("distanceToTSS").cast("double"),
                    f.col("score").cast("double"),
                    f.col("resourceScore"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(pmid).alias("pmid"),
                    f.col("studyId"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
