"""Interval dataset from EPIraction."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from gentropy.assets import schemas
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class IntervalsEpiraction:
    """Interval dataset from EPIraction."""

    @staticmethod
    def read(spark: SparkSession, path: str) -> DataFrame:
        """Read EPIraction dataset.

        Args:
            spark (SparkSession): Spark session
            path (str): Path to the dataset

        Returns:
            DataFrame: Raw, full EPIraction dataframe
        """
        input_schema = t.StructType.fromJson(
            json.loads(
                pkg_resources.read_text(schemas, "epiraction.json", encoding="utf-8")
            )
        )
        return (
            spark.read.option("delimiter", "\t")
            .option("mode", "DROPMALFORMED")
            .option("header", "true")
            .schema(input_schema)
            .csv(path)
        )

    @classmethod
    def parse(
        cls: type[IntervalsEpiraction],
        raw_epiraction_df: DataFrame,
        target_index: TargetIndex,
    ) -> Intervals:
        """Parse EPIraction dataset.

        Args:
            raw_epiraction_df (DataFrame): Raw EPIraction dataset
            target_index (TargetIndex): Target index

        Returns:
            Intervals: Intervals dataset
        """
        # Constant values:
        dataset_name = "epiraction"
        pmid = "40027634"  # PMID for the EPIraction paper

        # Read the epiraction file:
        parsed_epiraction_df = (
            raw_epiraction_df.filter(
                (f.col("class") == "enhancer") | (f.col("class") == "promoter")
            )
            .withColumn(
                "chromosome",
                f.regexp_replace(f.col("#chr"), "^chr", ""),
            )
            .withColumnRenamed("TargetGeneEnsemblID", "geneId")
            .withColumnRenamed("CellType", "biosampleName")
            .withColumnRenamed("Score", "score")
            .withColumnRenamed("class", "intervalType")
            .withColumn(
                "resourceScore",
                f.array(
                    f.struct(
                        f.lit("H3K27ac").alias("name"),
                        f.col("H3K27ac").cast("float").alias("value"),
                    ),
                    f.struct(
                        f.lit("Open").alias("name"),
                        f.col("Open").cast("float").alias("value"),
                    ),
                    f.struct(
                        f.lit("Cofactor").alias("name"),
                        f.col("Cofactor").cast("float").alias("value"),
                    ),
                    f.struct(
                        f.lit("CTCF").alias("name"),
                        f.col("CTCF").cast("float").alias("value"),
                    ),
                    f.struct(
                        f.lit("HiC_contacts").alias("name"),
                        f.col("HiC_contacts").cast("float").alias("value"),
                    ),
                    f.struct(
                        f.lit("abc_tissue").alias("name"),
                        f.col("abc_tissue").cast("float").alias("value"),
                    ),
                ),
            )
        )

        parsed_epiraction_df = parsed_epiraction_df.join(
            target_index._df.select(f.col("id").alias("geneId")),
            on="geneId",
            how="inner",
        )

        return Intervals(
            _df=(
                parsed_epiraction_df
                # Select relevant columns:
                .select(
                    f.col("chromosome"),
                    f.col("start"),
                    f.col("end"),
                    f.col("geneId"),
                    f.col("biosampleName"),
                    f.col("intervalType"),
                    f.col("score"),
                    f.col("resourceScore"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(pmid).alias("pmid"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
