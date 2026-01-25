"""Interval dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

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
    INVALID_CHROMOSOME = "Interval chromosome was not found in contig index"
    INVALID_RANGE = "Interval range exceeded chromosome bounds"
    AMBIGUOUS_INTERVAL_TYPE = (
        "Multiple interval types for the same (region, geneId) pair"
    )


class IntervalType(str, Enum):
    """Enum representing interval type."""

    PROMOTER = "promoter"
    ENHANCER = "enhancer"
    INTRAGENIC = "intragenic"
    INTERGENIC = "intergenic"
    GENIC = "genic"


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    id_cols = ["chromosome", "start", "end", "geneId", "studyId", "intervalType"]

    @classmethod
    def get_schema(cls: type[Intervals]) -> StructType:
        """Provides the schema for the Intervals dataset.

        Returns:
            StructType: Schema for the Intervals dataset
        """
        return parse_spark_schema("intervals.json")

    @classmethod
    def get_QC_column_name(cls: type[Intervals]) -> str:
        """Abstract method to get the QC column name. Assumes None unless overriden by child classes.

        Returns:
            str: QC column name.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[Intervals]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in IntervalQualityCheck}

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
            >>> data = [(100, 200, 'enhancer', 150),  # tss within interval
            ...         (300, 400, 'promoter', 350),  # promoter type always 0 distance
            ...         (500, 600, 'enhancer', 400),  # tss 100 bp away the istart
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
            .when(tss.isNull(), f.lit(None).cast(t.IntegerType()))
            .otherwise(f.least(f.abs(tss - istart), f.abs(tss - iend)))
        )

        return expr.cast(t.IntegerType()).alias("distanceToTss")

    @qc_test
    def validate_datasource_id(self: Intervals) -> Intervals:
        """Validate datasourceId in the Intervals dataset.

        Returns:
            Intervals: Intervals dataset with invalid datasourceId flagged.

        Example:
        ---
        >>> data = [("1", 100, 200, "UNKNOWN_ID", "promoter", "interval1"),
        ...         ("1", 150, 250, "E2G", "enhancer", "interval2"),
        ...         ("2", 300, 400, "epiraction", "intragenic", "interval3"),
        ...         ("2", 350, 450, "", "promoter", "interval4")]
        >>> schema = "chromosome STRING, start LONG, end LONG, datasourceId STRING, intervalType STRING, intervalId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_datasource_id()
        >>> validated_intervals.df.select("intervalId", "qualityControls").show(truncate=False)
        +----------+-------------------------------------------------------+
        |intervalId|qualityControls                                        |
        +----------+-------------------------------------------------------+
        |interval1 |[Project id could not be resolved to any known dataset]|
        |interval2 |[]                                                     |
        |interval3 |[]                                                     |
        |interval4 |[Project id could not be resolved to any known dataset]|
        +----------+-------------------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
        valid_df = self.df.withColumn(
            qc_column,
            self.update_quality_flag(
                f.col(qc_column),
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

        Examples:
        ---
        >>> contig_data = [("1", 0, 250),
        ...                ("2", 0, 200)]
        >>> contig_schema = "id STRING, start LONG, end LONG"
        >>> contig_df = spark.createDataFrame(data=contig_data, schema=contig_schema)
        >>> contig_index = ContigIndex(_df=contig_df)
        >>> data = [("UNKNOWN_CHR", 100, 200, "E2G", "promoter", "interval1"),
        ...         ("1", 150, 250, "E2G", "enhancer", "interval2"),
        ...        ("2", 300, 400, "E2G", "intragenic", "interval3")]
        >>> schema = "chromosome STRING, start LONG, end LONG, datasourceId STRING, intervalType STRING, intervalId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_interval_range(contig_index)
        >>> validated_intervals.df.select("intervalId", "qualityControls").show(truncate=False)
        +----------+---------------------------------------------------+
        |intervalId|qualityControls                                    |
        +----------+---------------------------------------------------+
        |interval1 |[Interval chromosome was not found in contig index]|
        |interval2 |[]                                                 |
        |interval3 |[Interval range exceeded chromosome bounds]        |
        +----------+---------------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
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
                qc_column,
                self.update_quality_flag(
                    f.col(qc_column),
                    # The chromosome is not canonical,
                    # resulting in empty contig bounds after left join
                    ((f.col("contigStart").isNull()) | (f.col("contigEnd").isNull())),
                    IntervalQualityCheck.INVALID_CHROMOSOME,
                ),
            )
            .withColumn(
                qc_column,
                self.update_quality_flag(
                    f.col(qc_column),
                    # interval Range exceeds bounds the contig range
                    (
                        (f.col("start") < f.col("contigStart"))
                        | (f.col("end") > f.col("contigEnd"))
                    ),
                    IntervalQualityCheck.INVALID_RANGE,
                ),
            )
            .drop("chrStart", "chrEnd", "contigStart", "contigEnd")
        )

        return Intervals(_df=valid_df, _schema=Intervals.get_schema())

    @qc_test
    def validate_target(self: Intervals, target_index: TargetIndex) -> Intervals:
        """Validate targets in the Intervals dataset.

        Args:
            target_index (TargetIndex): Target index.

        Returns:
            Intervals: Intervals dataset with invalid targets flagged.

        Examples:
        ---
        >>> target_data = [("ENSG1",), ("ENSG2",)]
        >>> target_schema = "id STRING"
        >>> target_df = spark.createDataFrame(data=target_data, schema=target_schema)
        >>> target_index = TargetIndex(_df=target_df)
        >>> data = [("1", 100, 200, "ENSG1", "E2G", "promoter", "interval1"),
        ...         ("1", 150, 250, "", "E2G", "enhancer", "interval2"),
        ...         ("2", 300, 400, "OTHER", "epiraction", "intragenic", "interval3")]
        >>> schema = "chromosome STRING, start LONG, end LONG, geneId STRING, datasourceId STRING, intervalType STRING, intervalId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_target(target_index)
        >>> validated_intervals.df.select("intervalId", "qualityControls").show(truncate=False)
        +----------+-----------------------------------------------------+
        |intervalId|qualityControls                                      |
        +----------+-----------------------------------------------------+
        |interval1 |[]                                                   |
        |interval2 |[Target/gene identifier could not match to reference]|
        |interval3 |[Target/gene identifier could not match to reference]|
        +----------+-----------------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
        gene_set = target_index.df.select(
            f.col("id").alias("geneId"), f.lit(True).alias("isIdFound")
        )
        validated_df = (
            self.df.join(gene_set, on="geneId", how="left")
            .withColumn(
                qc_column,
                self.update_quality_flag(
                    f.col(qc_column),
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

        Examples:
        ---
        >>> biosample_data = [("BS1", "name1"), ("BS2", "name2")]
        >>> biosample_schema = "biosampleId STRING, biosampleName STRING"
        >>> biosample_df = spark.createDataFrame(data=biosample_data, schema=biosample_schema)
        >>> biosample_index = BiosampleIndex(_df=biosample_df)
        >>> data = [("1", 100, 200, "E2G", "promoter", "interval1", "BS1"),
        ...         ("1", 150, 250, "E2G", "enhancer", "interval2", "UNKNOWN_BS")]
        >>> schema = "chromosome STRING, start LONG, end LONG, datasourceId STRING, intervalType STRING, intervalId STRING, biosampleId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_biosample(biosample_index)
        >>> validated_intervals.df.select("intervalId", "qualityControls").show(truncate=False)
        +----------+-----------------------------------------------------+
        |intervalId|qualityControls                                      |
        +----------+-----------------------------------------------------+
        |interval1 |[]                                                   |
        |interval2 |[Biosample identifier was not found in the reference]|
        +----------+-----------------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
        biosample_set = biosample_index.df.select(
            f.col("biosampleId"), f.lit(True).alias("isIdFound")
        )
        validated_df = (
            self.df.join(biosample_set, on="biosampleId", how="left")
            .withColumn(
                qc_column,
                self.update_quality_flag(
                    f.col(qc_column),
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

        Examples:
        ---
        >>> data = [("1", 100, 200, "ENSG1", "E2G", "promoter", "interval1"),
        ...         ("1", 150, 250, "ENSG2", "E2G", "enhancer", "interval2"),
        ...         ("2", 300, 400, "ENSG3", "E2G", "intragenic", "interval3"),
        ...         ("2", 300, 400, "ENSG3", "E2G", "genic", "interval4"),
        ...         ("2", 400, 500, "ENSG4", "E2G", "other", "interval5"),
        ...         ("2", 450, 550, "ENSG5", "E2G", "", "interval6")]
        >>> schema = "chromosome STRING, start LONG, end LONG, geneId STRING, datasourceId STRING, intervalType STRING, intervalId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_interval_type()
        >>> validated_intervals.df.select("intervalType", "qualityControls").show(truncate=False)
        +------------+------------------------------------------------------------+
        |intervalType|qualityControls                                             |
        +------------+------------------------------------------------------------+
        |promoter    |[]                                                          |
        |enhancer    |[]                                                          |
        |intragenic  |[Multiple interval types for the same (region, geneId) pair]|
        |genic       |[Multiple interval types for the same (region, geneId) pair]|
        |other       |[Interval type is not supported]                            |
        |            |[Interval type is not supported]                            |
        +------------+------------------------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
        valid_df = self.df.withColumn(
            qc_column,
            self.update_quality_flag(
                f.col(qc_column),
                ~f.col("intervalType").isin(
                    [interval_type.value for interval_type in IntervalType]
                ),
                IntervalQualityCheck.UNKNOWN_INTERVAL_TYPE,
            ),
        )

        window = Window.partitionBy("chromosome", "start", "end", "geneId")

        valid_df = valid_df.withColumn(
            qc_column,
            self.update_quality_flag(
                f.col(qc_column),
                f.size(f.collect_set("intervalType").over(window)) > 1,
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

        Examples:
        ---
        >>> data = [("1", 100, 200, "E2G", "promoter", 0.5, "interval1"),
        ...         ("1", 150, 250, "E2G", "enhancer", -1.0, "interval2"),
        ...         ("2", 300, 400, "E2G", "intragenic", 2.0, "interval3"),
        ...         ("2", 350, 450, "E2G", "promoter", None, "interval4")]
        >>> schema = "chromosome STRING, start LONG, end LONG, datasourceId STRING, intervalType STRING, score DOUBLE, intervalId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_score(min_score=0.0, max_score=1.0)
        >>> validated_intervals.df.select("intervalId", "qualityControls").show(truncate=False)
        +----------+-----------------------------------------------+
        |intervalId|qualityControls                                |
        +----------+-----------------------------------------------+
        |interval1 |[]                                             |
        |interval2 |[Score was above or below specified thresholds]|
        |interval3 |[Score was above or below specified thresholds]|
        |interval4 |[Score was above or below specified thresholds]|
        +----------+-----------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
        valid_df = self.df.withColumn(
            qc_column,
            self.update_quality_flag(
                f.col(qc_column),
                ~f.col("score").between(min_score, max_score) | f.col("score").isNull(),
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

        Examples:
        ---
        >>> data = [("1", 100, 200, "ENSG1", "S1", "BS1", "E2G", "promoter", 0.5, "interval1"),
        ...         ("1", 100, 200, "ENSG1", "S1", "BS1", "E2G", "promoter", 0.7, "interval2"),
        ...         ("2", 300, 400, "ENSG2", "S1", "BS2", "E2G", "enhancer", 0.9, "interval3")]
        >>> schema = "chromosome STRING, start LONG, end LONG, geneId STRING, studyId STRING, biosampleId STRING, datasourceId STRING, intervalType STRING, score DOUBLE, intervalId STRING"
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> intervals = Intervals(_df=df)
        >>> validated_intervals = intervals.validate_id_has_unique_score()
        >>> validated_intervals.df.select("intervalId", "qualityControls").show(truncate=False)
        +----------+-----------------------------------------------+
        |intervalId|qualityControls                                |
        +----------+-----------------------------------------------+
        |interval1 |[Interval has a duplicate with different score]|
        |interval2 |[Interval has a duplicate with different score]|
        |interval3 |[]                                             |
        +----------+-----------------------------------------------+
        <BLANKLINE>
        """
        qc_column = self.get_QC_column_name()
        if qc_column not in self.df.columns:
            self.df = self.df.withColumn(
                qc_column, f.array().cast(t.ArrayType(t.StringType()))
            )
        w = Window().partitionBy(
            "chromosome",
            "start",
            "end",
            "biosampleId",
            "geneId",
            "studyId",
            "intervalType",
        )
        valid_df = self.df.withColumn(
            qc_column,
            self.update_quality_flag(
                f.col(qc_column),
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
