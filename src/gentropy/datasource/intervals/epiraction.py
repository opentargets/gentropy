"""Interval dataset from EPIraction."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pyspark.sql.functions as f

from gentropy.common.processing import normalize_chromosome
from gentropy.dataset.intervals import IntervalDataSource, Intervals
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class IntervalsEpiraction:
    """Interval dataset from EPIraction."""

    PMID: ClassVar[str] = "40027634"

    @staticmethod
    def read(spark: SparkSession, path: str) -> DataFrame:
        """Read EPIraction dataset (tsv with header).

        Args:
            spark (SparkSession): Spark session
            path (str): Path to the EPIraction dataset (tsv files)

        Returns:
            DataFrame: Raw EPIraction DataFrame.
        """
        return (
            spark.read.option("delimiter", "\t")
            .option("mode", "DROPMALFORMED")
            .option("header", "true")
            .csv(path)
        )

    @classmethod
    def parse(
        cls: type[IntervalsEpiraction],
        raw_epiraction_df: DataFrame,
        target_index: TargetIndex,
    ) -> Intervals:
        """Parse EPIraction Intervals.

        Args:
            raw_epiraction_df (DataFrame): Raw EPIraction DataFrame.
            target_index (TargetIndex): Target index.

        Returns:
            Intervals: Parsed Intervals dataset.
        """
        base = (
            raw_epiraction_df.withColumn(
                "studyId",
                f.regexp_extract(f.input_file_name(), r"([^/]+)\.bed\.gz$", 1),
            )
            .withColumn("chromosome", normalize_chromosome(f.col("chr")))
            .withColumn("start", f.col("start").cast("long"))
            .withColumn("end", f.col("end").cast("long"))
            .withColumnRenamed("TargetGeneEnsemblID", "geneId")
            .withColumnRenamed("CellType", "biosampleName")
            .withColumnRenamed("Score", "score")
            .withColumnRenamed("class", "intervalType")
            .withColumn("intervalType", f.lower(f.trim(f.col("intervalType"))))
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
        tss_lut = target_index.tss_lut()

        joined = base.alias("iv").join(tss_lut.alias("ti"), on="geneId", how="left")
        parsed = (
            joined.withColumn(
                "distanceToTss",
                Intervals.distance_to_tss(
                    f.col("iv.start"),
                    f.col("iv.end"),
                    f.col("iv.intervalType"),
                    f.col("ti.tss"),
                ),
            )
            .withColumn(
                "intervalId",
                Intervals.generate_identifier(Intervals.id_cols),
            )
            .withColumn("qualityControls", f.array().cast("array<string>"))
        )

        return Intervals(
            _df=(
                parsed.select(
                    f.col("chromosome"),
                    f.col("start"),
                    f.col("end"),
                    f.col("geneId"),
                    f.col("biosampleName"),
                    f.col("intervalType"),
                    f.col("distanceToTss"),
                    f.col("score"),
                    f.col("resourceScore"),
                    f.lit(IntervalDataSource.EPIRACTION.value).alias("datasourceId"),
                    f.lit(cls.PMID).alias("pmid"),
                    f.col("studyId"),
                    f.col("intervalId"),
                    f.col("qualityControls"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
