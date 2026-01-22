"""Interval dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from pydantic import BaseModel
from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.contig_index import ContigIndex
from gentropy.dataset.dataset import Dataset, DatasetValidationResult, qc_test
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql.types import StructType


P = ParamSpec("P")
F = TypeVar("F", bound=Dataset)


class IntervalQCConfig(BaseModel):
    """Configuration for interval quality control."""

    min_valid_score: float
    max_valid_score: float


class IntervalDataSource(str, Enum):
    """Enum for interval data sources."""

    E2G = "E2G"
    EPIRACTION = "epiraction"


class IntervalQualityCheck(str, Enum):
    """Enum for interval quality check reasons."""

    UNRESOLVED_TARGET = "Target/gene identifier could not match to reference"
    UNKNOWN_BIOSAMPLE = "Biosample identifier was not found in the reference"
    SCORE_OUTSIDE_BOUNDS = "Score was above or below specified thresholds"
    UNKNOWN_INTERVAL_TYPE = "Interval type is not supported"
    AMBIGUOUS_SCORE = "Interval has a duplicate with different score"
    UNKNOWN_PROJECT_ID = "Project id could not be resolved to any known dataset"
    INVALID_CHROMOSOME = "Interval chromosome was not found in "
    INVALID_RANGE = "Interval range exceeded chromosome bounds"
    AMBIGUOUS_INTERVAL_TYPE = "Multiple interval types for the same (id, geneId) pair"


class IntervalType(str, Enum):
    """Enum representing interval type."""

    PROMOTER = "promoter"
    ENHANCER = "enhancer"
    INTRAGENIC = "intragenic"


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    id_cols = ["chromosome", "start", "end", "geneId", "studyId", "intervalType"]

    @staticmethod
    def distance_to_tss(
        istart: Column, iend: Column, itype: Column, tss: Column
    ) -> Column:
        """Compute distance from interval to TSS.

        Args:
            istart (Column): Interval start position.
            iend (Column): Interval end position.
            itype (Column): Interval type.
            tss (Column): Transcription start site position.

        Returns:
            Column: Distance from interval to TSS.

        Example:
            >>> data = [(100, 200, 'enhancer', 150), # tss within interval
            ...         (300, 400, 'promoter', 350), # promoter type always 0 distance
            ...         (500, 600, 'enhancer', 400), # tss 100 bp away the istart
            ...         (700, 800, 'enhancer', None)] # tss is null
            >>> df = spark.createDataFrame(data, ['istart', 'iend', 'itype', 'tss'])
            >>> df.withColumn('distanceToTss', Intervals.distance_to_tss(
            ...     f.col('istart'), f.col('iend'), f.col('itype'), f.col('tss'))
            ... ).show()
            +------+----+--------+----+-------------+
            |istart|iend|   itype| tss|distanceToTss|
            +------+----+--------+----+-------------+
            |   100| 200|enhancer| 150|            0|
            |   300| 400|promoter| 350|            0|
            |   500| 600|enhancer| 400|          100|
            |   700| 800|enhancer|NULL|         NULL|
            +------+----+--------+----+-------------+
            <BLANKLINE>
        """
        is_promoter = itype == f.lit(IntervalType.PROMOTER.value)
        tss_in_interval = (tss >= istart) & (tss <= iend)

        expr = (
            f.when((is_promoter) | (tss_in_interval), f.lit(0))
            .when(tss.isNull(), f.lit(None).cast(t.LongType()))
            .otherwise(f.least(f.abs(tss - istart), f.abs(tss - iend)))
        )

        return expr.cast(t.LongType()).alias("distanceToTss")

    @classmethod
    def get_schema(cls: type[Intervals]) -> StructType:
        """Provides the schema for the Intervals dataset.

        Returns:
            StructType: Schema for the Intervals dataset
        """
        return parse_spark_schema("intervals.json")

    @qc_test
    def validate_datasource_id(self: Intervals) -> Intervals:
        """Validate datasourceId in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with invalid datasourceId flagged.
        """
        valid_df = self.df.withColumn(
            "qualityControls",
            self.update_quality_flag(
                f.col("qualityControls"),
                ~f.col("datasourceId").isin([ds.value for ds in IntervalDataSource]),
                IntervalQualityCheck.UNKNOWN_PROJECT_ID,
            ),
        )
        return Intervals(_df=valid_df)

    @qc_test
    def validate_interval_range(
        self: Intervals, contig_index: ContigIndex
    ) -> Intervals:
        """Validate chromosome labels in the Intervals dataset.

        Args:
            contig_index (ContigIndex): Contig index.

        Returns:
            Intervals: Intervals dataset with invalid chromosome labels flagged.
        """
        chromosomes = f.broadcast(
            contig_index.canonical().df.select(
                f.col("start").alias("contigStart"),
                f.col("end").alias("contigEnd"),
                f.col("id").alias("chromosome"),
            )
        )
        valid_df = (
            self.df.repartitionByRange("chromosome")
            .join(chromosomes, on="chromosome", how="left")
            .withColumn(
                "qualityControls",
                self.update_quality_flag(
                    f.col("qualityControls"),
                    # The chromosome is not canonical, resulting in empty contig bounds after left join
                    ((f.col("contigStat").isNull()) | (f.col("contigEnd").isNull())),
                    IntervalQualityCheck.INVALID_CHROMOSOME,
                ),
            )
            .withColumn(
                "qualityControls",
                self.update_quality_flag(
                    f.col("qualityControls"),
                    # interval Range exceeds bounds the contig range
                    (
                        (f.col("start") < f.col("contigStart"))
                        | (f.col("end") > f.col("contigEnd"))
                    ),
                    IntervalQualityCheck.INVALID_RANGE,
                ),
            )
            .drop("chrStart", "chrEnd")
        )

        return Intervals(_df=valid_df, _schema=Intervals.get_schema())

    @qc_test
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

    @qc_test
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

    @qc_test
    def validate_interval_type(self: Intervals) -> Intervals:
        """Validate interval types in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with invalid interval types flagged.
        """
        valid_df = self.df.withColumn(
            "qualityControls",
            self.update_quality_flag(
                f.col("qualityControls"),
                ~f.col("intervalType").isin(
                    [interval_type.value for interval_type in IntervalType]
                ),
                IntervalQualityCheck.UNKNOWN_INTERVAL_TYPE,
            ),
        )

        window = Window.partitionBy("id", "geneId")

        valid_df = valid_df.withColumn(
            "qualityControls",
            self.update_quality_flag(
                f.col("qualityControls"),
                f.count_distinct("intervalType").over(window) > 1,
                IntervalQualityCheck.AMBIGUOUS_INTERVAL_TYPE,
            ),
        )

        return Intervals(_df=valid_df)

    @qc_test
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

    @qc_test
    def validate_id_has_unique_score(self: Intervals) -> Intervals:
        """Validate unique (id, score) group.

        The assumption is that the same id (geneId, biosampleId) should not have different

        Returns:
            Intervals: Intervals dataset with ambiguous scores flagged.
        """
        w = Window().partitionBy(
            "id", "biosampleId", "geneId", "studyId", "intervalType"
        )
        valid_df = self.df.withColumn(
            "qualityControls",
            self.update_quality_flag(
                f.col("qualityControls"),
                (f.size(f.array_distinct(f.collect_list(f.col("score")).over(w))) > 1),
                IntervalQualityCheck.AMBIGUOUS_SCORE,
            ),
        )

        return Intervals(_df=valid_df)

    def qc(
        self,
        contig_index: ContigIndex,
        target_index: TargetIndex,
        biosample_index: BiosampleIndex,
        min_valid_score: float,
        max_valid_score: float,
        invalid_qc_reasons: list[str] | None = None,
    ) -> DatasetValidationResult[Intervals]:
        """Perform Quality Control over Intervals dataset.

        Args:
            contig_index (ContigIndex): Contig index.
            target_index (TargetIndex): Target index.
            biosample_index (BiosampleIndex): Biosample index.
            min_valid_score (float): Minimum valid score for interval QC.
            max_valid_score (float): Maximum valid score for interval QC.
            invalid_qc_reasons (list[str] | None): List of invalid quality check reason names from `IntervalQualityCheck` (e.g. ['INVALID_CHROMOSOME']).

        Returns:
            DatasetValidationResult[Intervals]: Valid and invalid Intervals datasets.
        """
        if invalid_qc_reasons is None:
            invalid_qc_reasons = []
        return (
            self.validate_datasource_id()
            .validate_interval_range(contig_index)
            .validate_target(target_index)
            .validate_biosample(biosample_index)
            .validate_interval_type()
            .validate_score(min_valid_score, max_valid_score)
            .validate_id_has_unique_score()
            .persist()
            .valid_rows(invalid_qc_reasons)
        )
