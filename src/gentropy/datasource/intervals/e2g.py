"""Interval dataset from regulatory Enhancer To Gene (rE2G)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pyspark.sql.functions as f

from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class IntervalsE2G:
    """Interval dataset from E2G."""

    DATASET_NAME: ClassVar[str] = "E2G"
    PMID: ClassVar[str] = "38014075"  # PMID for the E2G paper
    VALID_INTERVAL_TYPES: ClassVar[list[str]] = ["promoter", "genic", "intergenic"]

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
        biosample_index: BiosampleIndex,
    ) -> Intervals:
        """Parse E2G dataset.

        Args:
            raw_e2g_df (DataFrame): Raw E2G DataFrame.
            biosample_mapping (DataFrame): Biosample mapping DataFrame.
            target_index (TargetIndex): Target index.
            biosample_index (BiosampleIndex): Biosample index.

        Returns:
            Intervals: Parsed Intervals dataset.
        """
        base = (
            raw_e2g_df.withColumn(
                "studyId", f.regexp_extract(f.col("file_path"), r"([^/]+)\.bed\.gz$", 1)
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
            .withColumn("start", f.col("start").cast("long"))
            .withColumn("end", f.col("end").cast("long"))
        )

        base = base.withColumn(
            "intervalType", f.lower(f.trim(f.col("intervalType")))
        ).filter(f.col("intervalType").isin(cls.VALID_INTERVAL_TYPES))

        # Target Index: preferred TSS + fallbacks (canonical transcript, genomicLocation)
        ti = target_index._df.select(
            f.col("id").alias("geneId"),
            f.col("tss").cast("long").alias("tss_primary"),
            f.col("canonicalTranscript.chromosome").alias("ct_chr"),
            f.col("canonicalTranscript.start").cast("long").alias("ct_start"),
            f.col("canonicalTranscript.end").cast("long").alias("ct_end"),
            f.col("canonicalTranscript.strand").alias("ct_strand"),
            f.col("genomicLocation.chromosome").alias("gl_chr"),
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
            "tss_coalesced", f.coalesce(f.col("tss_primary"), ct_tss, gl_tss)
        )

        # Join & recompute distanceToTss
        joined = base.alias("iv").join(
            ti_with_tss.alias("ti"), on="geneId", how="inner"
        )
        tss = f.col("ti.tss_coalesced")

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

        parsed = (
            joined.withColumn("distanceToTss", distance_expr.cast("double"))
            .withColumn(
                "intervalId",
                f.sha1(
                    f.concat_ws("_", "chromosome", "start", "end", "geneId", "studyId")
                ),
            )
            .join(
                biosample_mapping.select("biosampleName", "biosampleId"),
                on="biosampleName",
                how="left",
            )
            .join(
                biosample_index.df.select("biosampleId"), on="biosampleId", how="inner"
            )
        )

        return Intervals(
            _df=(
                parsed.select(
                    f.col("chromosome"),
                    f.col("start").cast("string"),
                    f.col("end").cast("string"),
                    f.col("geneId"),
                    f.col("biosampleName"),
                    f.col("intervalType"),
                    f.col("distanceToTss").cast("integer"),
                    f.col("score").cast("double"),
                    f.col("resourceScore"),
                    f.lit(cls.DATASET_NAME).alias("datasourceId"),
                    f.lit(cls.PMID).alias("pmid"),
                    f.col("studyId"),
                    f.col("biosampleId"),
                    f.col("intervalId"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
