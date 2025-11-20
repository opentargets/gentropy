"""Interval dataset from EPIraction."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pyspark.sql.functions as f

from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class IntervalsEpiraction:
    """Interval dataset from EPIraction."""

    DATASET_NAME: ClassVar[str] = "epiraction"
    PMID: ClassVar[str] = "40027634"
    VALID_INTERVAL_TYPES: ClassVar[list[str]] = ["promoter", "enhancer"]

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
            raw_epiraction_df.filter(f.col("class").isin(cls.VALID_INTERVAL_TYPES))
            .withColumn("chromosome", f.regexp_replace(f.col("chr"), r"^chr", ""))
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
            .withColumn("start", f.col("start").cast("long"))
            .withColumn("end", f.col("end").cast("long"))
            .withColumn("intervalType", f.lower(f.trim(f.col("intervalType"))))
        )

        # Target Index: preferred TSS (+ fallbacks)
        ti = target_index._df.select(
            f.col("id").alias("geneId"),
            f.col("tss").cast("long").alias("tss_primary"),
            f.col("canonicalTranscript.start").cast("long").alias("ct_start"),
            f.col("canonicalTranscript.end").cast("long").alias("ct_end"),
            f.col("canonicalTranscript.strand").alias("ct_strand"),
            f.col("genomicLocation.start").cast("long").alias("gl_start"),
            f.col("genomicLocation.end").cast("long").alias("gl_end"),
            f.col("genomicLocation.strand").cast("int").alias("gl_strand"),
        )

        ct_tss = f.when(f.col("ct_strand") == "+", f.col("ct_start")).when(
            f.col("ct_strand") == "-", f.col("ct_end")
        )
        gl_tss = f.when(f.col("gl_strand") == 1, f.col("gl_start")).when(
            f.col("gl_strand") == -1, f.col("gl_end")
        )

        ti_with_tss = ti.withColumn(
            "tss_from_target_index", f.coalesce(f.col("tss_primary"), ct_tss, gl_tss)
        )

        has_input_tss = "distanceToTSS" in base.columns
        base_with_fallback = (
            base.withColumn("tss_from_input", f.col("distanceToTSS").cast("long"))
            if has_input_tss
            else base.withColumn("tss_from_input", f.lit(None).cast("long"))
        )

        joined = base_with_fallback.alias("iv").join(
            ti_with_tss.alias("ti"), on="geneId", how="inner"
        )

        tss = f.coalesce(f.col("ti.tss_from_target_index"), f.col("iv.tss_from_input"))

        dist_core = f.when(
            (tss >= f.col("iv.start")) & (tss <= f.col("iv.end")), f.lit(0)
        ).otherwise(
            f.least(f.abs(tss - f.col("iv.start")), f.abs(tss - f.col("iv.end")))
        )
        distance_expr = (
            f.when(f.col("iv.intervalType") == "promoter", f.lit(0))
            .when(tss.isNull(), f.lit(None).cast("long"))
            .otherwise(dist_core)
        )

        parsed = joined.withColumn(
            "distanceToTss", distance_expr.cast("double")
        ).withColumn(
            "intervalId",
            f.sha1(
                f.concat_ws(
                    "_",
                    f.col("iv.chromosome"),
                    f.col("iv.start"),
                    f.col("iv.end"),
                    f.col("iv.geneId"),
                    f.lit(cls.DATASET_NAME),
                )
            ),
        )

        return Intervals(
            _df=(
                parsed.select(
                    f.col("iv.chromosome").alias("chromosome"),
                    f.col("iv.start").cast("string").alias("start"),
                    f.col("iv.end").cast("string").alias("end"),
                    f.col("iv.geneId").alias("geneId"),
                    f.col("iv.biosampleName").alias("biosampleName"),
                    f.col("iv.intervalType").alias("intervalType"),
                    f.col("distanceToTss").cast("integer").alias("distanceToTss"),
                    f.col("iv.score").cast("double").alias("score"),
                    f.col("iv.resourceScore").alias("resourceScore"),
                    f.lit(cls.DATASET_NAME).alias("datasourceId"),
                    f.lit(cls.PMID).alias("pmid"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
