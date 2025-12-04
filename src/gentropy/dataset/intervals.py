"""Interval dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

from enum import Enum


class IntervalQualityCheck(Enum):
    """Enum for interval quality check reasons."""

    UNRESOLVED_TARGET = "Target/gene identifier could not match to reference"
    UNKNOWN_BIOSAMPLE = "Biosample identifier was not found in the reference"
    SCORE_OUTSIDE_BOUNDS = "Score was above or below specified thresholds"


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    @classmethod
    def get_schema(cls: type[Intervals]) -> StructType:
        """Provides the schema for the Intervals dataset.

        Returns:
            StructType: Schema for the Intervals dataset
        """
        return parse_spark_schema("intervals.json")

    @classmethod
    def from_source(
        cls: type[Intervals],
        spark: SparkSession,
        source_name: str,
        source_path: str,
        target_index: TargetIndex,
        biosample_index: BiosampleIndex,
        biosample_mapping: DataFrame,
    ) -> Intervals:
        """Collect interval data for a particular source.

        Args:
            spark (SparkSession): Spark session
            source_name (str): Name of the interval source
            source_path (str): Path to the interval source file
            target_index (TargetIndex): Target index
            biosample_index (BiosampleIndex): Biosample index
            biosample_mapping (DataFrame): Biosample mapping DataFrame

        Returns:
            Intervals: Intervals dataset

        Raises:
            ValueError: If the source name is not recognised
        """
        from gentropy.datasource.intervals.e2g import IntervalsE2G
        from gentropy.datasource.intervals.epiraction import IntervalsEpiraction

        if source_name == "e2g":
            raw = IntervalsE2G.read(spark, source_path)
            return IntervalsE2G.parse(
                raw_e2g_df=raw,
                biosample_mapping=biosample_mapping,
                target_index=target_index,
                biosample_index=biosample_index,
            )

        if source_name == "epiraction":
            raw = IntervalsEpiraction.read(spark, source_path)
            return IntervalsEpiraction.parse(
                raw_epiraction_df=raw,
                target_index=target_index,
            )

        raise ValueError(f"Unknown interval source: {source_name!r}")

    def validate_target(self: Intervals, target_index: TargetIndex) -> Intervals:
        """Validate targets in the Intervals dataset.

        Args:
            target_index (TargetIndex): Target index.

        Returns:
            Intervals: Intervals dataset with invalid targets flagged.
        """
        if "qualityControls" not in self.df.columns:
            self.df = self.df.withColumn(
                "qualityControls", f.array().cast(t.ArrayType(t.StringType()))
            )
        gene_set = target_index.df.select(
            f.col("id").alias("geneId"), f.lit(True).alias("isIdFound")
        )
        validated_df = (
            self.df.join(gene_set, on="geneId", how="left")
            .withColumn(
                "qualityControls",
                self.update_quality_flag(
                    f.col("qualityControls"),
                    f.col("isIdFound").isNull(),
                    IntervalQualityCheck.UNRESOLVED_TARGET,
                ),
            )
            .drop("isIdFound")
        )
        return Intervals(_df=validated_df, _schema=Intervals.get_schema())

    def validate_biosample(
        self: Intervals, biosample_index: BiosampleIndex
    ) -> Intervals:
        """Validate biosamples in the Intervals dataset.

        Args:
            biosample_index (BiosampleIndex): Biosample index.

        Returns:
            Intervals: Intervals dataset with invalid biosamples flagged.
        """
        if "qualityControls" not in self.df.columns:
            self.df = self.df.withColumn(
                "qualityControls", f.array().cast(t.ArrayType(t.StringType()))
            )
        biosample_set = biosample_index.df.select(
            f.col("id").alias("biosampleId"), f.lit(True).alias("isIdFound")
        )
        validated_df = (
            self.df.join(biosample_set, on="biosampleId", how="left")
            .withColumn(
                "qualityControls",
                self.update_quality_flag(
                    f.col("qualityControls"),
                    f.col("isIdFound").isNull(),
                    IntervalQualityCheck.UNKNOWN_BIOSAMPLE,
                ),
            )
            .drop("isIdFound")
        )
        return Intervals(_df=validated_df, _schema=Intervals.get_schema())

    def validate_score(
        self: Intervals, min_score: float, max_score: float
    ) -> Intervals:
        """Validate scores in the Intervals dataset.

        Args:
            min_score (float): Minimum acceptable score.
            max_score (float): Maximum acceptable score.

        Returns:
            Intervals: Intervals dataset with invalid scores flagged.

        """
        valid_df = self.df.withColumn(
            "qualityControls",
            self.update_quality_flag(
                f.col("qualityControls"),
                ~f.col("score").between(min_score, max_score),
                IntervalQualityCheck.SCORE_OUTSIDE_BOUNDS,
            ),
        )
        return Intervals(_df=valid_df)

    def validate_interval(self: Intervals) -> Intervals:
        """Validate chromosome labels in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with invalid chromosome labels flagged.
        """
        raise NotImplementedError("Chromosome label validation not yet implemented.")

    def validate_unique_id(self: Intervals) -> Intervals:
        """Validate unique IDs in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with duplicate IDs flagged.
        """
        raise NotImplementedError("Unique ID validation not yet implemented.")

    def validate_interval_type(self: Intervals) -> Intervals:
        """Validate interval types in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with invalid interval types flagged.
        """
        raise NotImplementedError("Interval type validation not yet implemented.")

    def validate_biofeature(self: Intervals) -> Intervals:
        """Validate biofeature in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with invalid biofeatures flagged.
        """
        raise NotImplementedError("Biofeature validation not yet implemented.")
