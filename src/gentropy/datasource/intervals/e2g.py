"""Interval dataset from regulatory Enhancer To Gene (rE2G)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import normalize_chromosome
from gentropy.dataset.intervals import IntervalDataSource, Intervals
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class IntervalsE2G:
    """Interval dataset from E2G."""

    PMID: ClassVar[str] = "38014075"  # PMID for the E2G paper

    @staticmethod
    def read(spark: SparkSession, path: str) -> DataFrame:
        """Read E2G dataset.

        Args:
            spark (SparkSession): Spark session
            path (str): Path to the E2G dataset (tsv.gz files)

        Returns:
            DataFrame: Raw E2G DataFrame.
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
    ) -> Intervals:
        """Parse E2G dataset.

        Args:
            raw_e2g_df (DataFrame): Raw E2G DataFrame.
            biosample_mapping (DataFrame): Biosample mapping DataFrame.
            target_index (TargetIndex): Target index.

        Returns:
            Intervals: Parsed Intervals dataset.
        """
        base = (
            raw_e2g_df.withColumn(
                "studyId", f.regexp_extract(f.col("file_path"), r"([^/]+)\.bed\.gz$", 1)
            )
            .withColumn("chromosome", normalize_chromosome(f.col("chr")))
            .withColumn("start", f.col("start").cast("long"))
            .withColumn("end", f.col("end").cast("long"))
            .withColumnRenamed("TargetGeneEnsemblID", "geneId")
            .withColumnRenamed("CellType", "biosampleName")
            .withColumnRenamed("Score", "score")
            .withColumn("score", f.col("score").cast("double"))
            .withColumnRenamed("class", "intervalType")
            .withColumn("intervalType", f.lower(f.trim(f.col("intervalType"))))
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

        tss_lut = target_index.tss_lut()
        biosample_lut = biosample_mapping.select("biosampleName", "biosampleId")
        # Add distance to TSS from interval bounds
        joined = (
            base.alias("iv")
            .join(tss_lut.alias("ti"), on="geneId", how="left")
            .join(biosample_lut.alias("bl"), on="biosampleName", how="left")
        )
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
                    f.col("score"),
                    f.col("distanceToTss"),
                    f.col("resourceScore"),
                    f.lit(IntervalDataSource.E2G.value).alias("datasourceId"),
                    f.col("intervalType"),
                    f.lit(cls.PMID).alias("pmid"),
                    f.lit(None).cast(t.StringType()).alias("biofeature"),
                    f.col("biosampleName"),
                    f.lit(None).cast(t.StringType()).alias("biosampleFromSourceId"),
                    f.col("biosampleId"),
                    f.col("studyId"),
                    f.col("intervalId"),
                    f.col("qualityControls"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
