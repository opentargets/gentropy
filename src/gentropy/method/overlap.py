from enum import StrEnum

from pydantic import BaseModel, model_validator
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f


class UnknownPartitionStrategyError(Exception):
    """Exception raised for unknown partition strategies."""

    pass


class PartitionStrategyRegistry(StrEnum):
    INPUT_BASED = "input_based"
    """Overlap partitioning strategy that partitions the input exploded variants into approx equal size partitions, preserving all duplicates of variant in the same partition."""
    OUTPUT_BASED = "output_based"
    """Overlap partitioning strategy that partitions the input exploded variants based on the expected estimated amount of overlaps produced by the partition."""
    CUMULATIVE_OVERLAP_BASED = "cumulative_overlap_based"
    """Overlap partitioning strategy that partitions the input exploded variants based on the cumulative amount of overlaps produced by the partition."""
    CUMULATIVE_OVERLAP_BASED_2 = "cumulative_overlap_based_2"
    """An improved version of cumulative overlap based partition strategy that takes into account the non-overlapping variants that also contribute to the output size of the partition."""
    REGION_CUMULATIVE_OVERLAP_BASED = "region_cumulative_overlap_based"
    """Overlap partitioning strategy using a genomic range join at locus level to find candidate overlapping pairs, then array_intersect for exact counts, feeding into cumulative bucket assignment. Avoids the O(N_loci²) crossJoin and the hot-variant skew of variant-level equi-joins."""


class CumulativeOverlapBasedPartition2(BaseModel):
    max_cumulative_overlap_output_size: int = 100
    """The maximum expected cumulative output size of overlaps for each partition in bytes. If not provided, the partitioning will be done solely based on the input size."""

    @model_validator(mode="after")
    def validate_max_cumulative_overlap_output_size(self):
        if (
            self.max_cumulative_overlap_output_size is None
            or self.max_cumulative_overlap_output_size <= 1
        ):
            raise ValueError(
                "max_cumulative_overlap_output_size must be a positive integer (at least 1)."
            )
        return self

    @property
    def _work_cols(self):
        return [
            "variantCount",
            "locusOverlapCount",
            "rowsPerVariant",
            "rowNumberOverVariantId",
            "sparseRowsPerVariant",
            "cumulativeSparseRowsPerVariant",
            "bucket",
        ]

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.filter(f.col("studyType").isNotNull())

        overlap_lookup = df.select(
            "studyLocusId",
            f.transform(
                "locus", lambda x: x.getField("variantId").alias("tagVariantId")
            ).alias("locus"),
        )
        pre_overlap = (
            overlap_lookup.alias("left")
            .crossJoin(overlap_lookup.alias("right"))
            .filter(f.col("left.studyLocusId") != f.col("right.studyLocusId"))
            .withColumn(
                "overlapSize", f.size(f.array_intersect("left.locus", "right.locus"))
            )
            .select(
                "left.studyLocusId",
                "overlapSize",
                f.size("left.locus").alias("leftSize"),
                f.size("right.locus").alias("rightSize"),
                f.col("right.studyLocusId").alias("rightStudyLocusId"),
            )
            .withColumn(
                "estimatedOutputSize",
                f.when(
                    f.col("overlapSize") > 0,
                    f.col("leftSize") + f.col("rightSize") - f.col("overlapSize"),
                ).otherwise(f.lit(0)),
            )
            # Amount of records that overlap will produce from the right locus
            .withColumn(
                "isOverlapping",
                f.when(f.col("overlapSize") > 0, f.lit(1)).otherwise(f.lit(0)),
            )
        )
        pre_overlap_cumulative = pre_overlap.groupBy("left.studyLocusId").agg(
            f.sum(f.col("isOverlapping")).alias("locusOverlapCount")
        )

        # rejoin
        df_with_estimated = df.join(
            pre_overlap_cumulative, on=["studyLocusId"], how="left"
        )

        w1 = (
            Window.partitionBy("tagVariantId")
            .orderBy("studyLocusId", "locusOverlapCount")
            .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        w2 = Window.partitionBy("chromosome").orderBy(
            "tagVariantId", "studyLocusId", "locusOverlapCount"
        )
        w3 = Window.partitionBy("tagVariantId").orderBy(
            "studyLocusId", "locusOverlapCount"
        )

        result = (
            df_with_estimated.withColumn("locusSize", f.size("locus"))
            .withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
                f.col("locusOverlapCount"),
            )
            .withColumn("variantCount", f.count(f.col("tagVariantId")).over(w1))
            .withColumn(
                "rowsPerVariant",
                f.sum(f.col("locusOverlapCount")).over(w1)
                - (f.col("variantCount") * (f.col("variantCount") - 1) / 2),
            )
            # Zero out non-first rowsPerVariant over the tagVariantId partition so we can use cumulative sum to get buckets
            .withColumn("rowNumberOverVariantId", f.row_number().over(w3))
            .withColumn(
                "sparseRowsPerVariant",
                f.when(
                    f.col("rowNumberOverVariantId") == 1, f.col("rowsPerVariant")
                ).otherwise(0),
            )
            .withColumn(
                "cumulativeSparseRowsPerVariant",
                f.sum(f.col("sparseRowsPerVariant")).over(w2),
            )
            .withColumn(
                "bucket",
                f.ceil(
                    f.col("cumulativeSparseRowsPerVariant")
                    / f.lit(self.max_cumulative_overlap_output_size)
                ),
            )
            .withColumn(
                "partition", f.concat_ws("_", f.col("chromosome"), f.col("bucket"))
            )
            .withColumn(
                "strategy",
                f.lit(PartitionStrategyRegistry.CUMULATIVE_OVERLAP_BASED.value),
            )
        )
        return result


class CumulativeOverlapBasedPartition(BaseModel):
    max_cumulative_overlap_output_size: int = 100
    """The maximum expected cumulative output size of overlaps for each partition in bytes. If not provided, the partitioning will be done solely based on the input size."""

    @model_validator(mode="after")
    def validate_max_cumulative_overlap_output_size(self):
        if (
            self.max_cumulative_overlap_output_size is None
            or self.max_cumulative_overlap_output_size <= 1
        ):
            raise ValueError(
                "max_cumulative_overlap_output_size must be a positive integer (at least 1)."
            )
        return self

    @property
    def _work_cols(self):
        # return ["locusRowCount","variantCount", "rank", "rowNumberOverVariantId", "sumLocusRowCount", "sparseVariantCount", "variantRowCount", "cumulativeOutputRowCount", "bucket"]
        return [
            "variantCount",
            "rank",
            "locusRowCount",
            "locusOverlapCount",
            "locusRightRowCount",
            "rowNumberOverVariantId",
            "sumLocusRowCount",
            "sparseLocusRowCount",
            "cumulativeOutputRowCount",
            "bucket",
            "rowsPerVariant",
        ]

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.filter(f.col("studyType").isNotNull())

        w = Window.partitionBy("studyLocusId", "rightStudyLocusId").orderBy(
            "overlapSize", "leftSize", "rightSize"
        )
        ww = Window.partitionBy("studyLocusId", "rightStudyLocusId").rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )

        overlap_lookup = df.select(
            "studyLocusId",
            f.transform(
                "locus", lambda x: x.getField("variantId").alias("tagVariantId")
            ).alias("locus"),
        )
        pre_overlap = (
            overlap_lookup.alias("left")
            .crossJoin(overlap_lookup.alias("right"))
            .filter(f.col("left.studyLocusId") != f.col("right.studyLocusId"))
            .withColumn(
                "overlapSize", f.size(f.array_intersect("left.locus", "right.locus"))
            )
            .select(
                "left.studyLocusId",
                "overlapSize",
                f.size("left.locus").alias("leftSize"),
                f.size("right.locus").alias("rightSize"),
                f.col("right.studyLocusId").alias("rightStudyLocusId"),
            )
            .withColumn(
                "estimatedOutputSize",
                f.when(
                    f.col("overlapSize") > 0,
                    f.col("leftSize") + f.col("rightSize") - f.col("overlapSize"),
                ).otherwise(f.lit(0)),
            )
            # .withColumn("aaa", f.when(f.col("overlapSize") > 0,f.col("leftSize") - f.col("overlapSize")).otherwise(f.lit(0)))
            # .withColumn("bbb", f.when(f.col("overlapSize") > 0,f.col("rightSize") - f.col("overlapSize")).otherwise(f.lit(0)))
            # .withColumn("row_number", f.row_number().over(w))
            # Amount of records that overlap will produce from the right locus
            .withColumn(
                "nNonOverlappingVariants",
                f.when(
                    f.col("overlapSize") > 0, f.col("rightSize") - f.col("overlapSize")
                ).otherwise(f.lit(0)),
            )
            # if there is an overlap, it will produce at least 1 record of overlap, if no overlap, it will produce 0 record of overlap
            .withColumn(
                "isOverlapping",
                f.when(f.col("overlapSize") > 0, f.lit(1)).otherwise(f.lit(0)),
            )
        )
        pre_overlap.filter(
            f.col("left.studyLocusId") == "e8a655d0ee2044f274498e5c55e3dcd5"
        ).show(truncate=False)
        pre_overlap_cumulative = pre_overlap.groupBy("left.studyLocusId").agg(
            f.sum(f.col("estimatedOutputSize")).alias("locusRowCount"),
            f.sum(f.col("nNonOverlappingVariants")).alias("locusRightRowCount"),
            f.sum(f.col("isOverlapping")).alias("locusOverlapCount"),
        )

        # rejoin
        df_with_estimated = df.join(
            pre_overlap_cumulative, on=["studyLocusId"], how="left"
        )

        w1 = (
            Window.partitionBy("tagVariantId")
            .orderBy("studyLocusId", "locusRowCount")
            .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        w2 = Window.partitionBy("chromosome").orderBy(
            "tagVariantId", "studyLocusId", "locusRowCount"
        )
        w3 = Window.partitionBy("tagVariantId").orderBy("studyLocusId", "locusRowCount")

        result = (
            df_with_estimated.withColumn("locusSize", f.size("locus"))
            .withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
                f.col("locusSize"),
                f.col("locusRowCount"),
                # f.col("locusLeftRowCount"),
                f.col("locusRightRowCount"),
                f.col("locusOverlapCount"),
            )
            .withColumn("variantCount", f.count(f.col("tagVariantId")).over(w1))
            .withColumn(
                "sumLocusRowCount",
                f.sum(f.col("locusRowCount") / f.col("locusSize")).over(w1),
            )
            .withColumn("rowNumberOverVariantId", f.row_number().over(w3))
            .withColumn("rank", f.rank().over(w2))
            # # Zero non-first cumulativeEstimatedOutputSize so we can use cumulative sum to get buckets
            .withColumn(
                "sparseLocusRowCount",
                f.when(
                    f.col("rowNumberOverVariantId") == 1, f.col("sumLocusRowCount")
                ).otherwise(0),
            )
            .withColumn(
                "rowsPerVariant",
                f.sum(f.col("locusOverlapCount")).over(w1)
                - (f.col("variantCount") * (f.col("variantCount") - 1) / 2),
            )
            .withColumn(
                "cumulativeOutputRowCount", f.sum(f.col("sparseLocusRowCount")).over(w2)
            )
            .withColumn(
                "bucket",
                f.ceil(
                    f.col("cumulativeOutputRowCount")
                    / f.lit(self.max_cumulative_overlap_output_size)
                ),
            )
            .withColumn(
                "partition", f.concat_ws("_", f.col("chromosome"), f.col("bucket"))
            )
            .withColumn(
                "strategy",
                f.lit(PartitionStrategyRegistry.CUMULATIVE_OVERLAP_BASED.value),
            )
        )
        return result


class InputBasedPartition(BaseModel):
    max_shuffle_partition_size: int = 10
    """The maximum size of each shuffle partition in bytes. If not provided, the partition size will be determined by Spark's default settings."""

    @model_validator(mode="after")
    def validate_max_shuffle_partition_size(self):
        if (
            self.max_shuffle_partition_size is None
            or self.max_shuffle_partition_size <= 1
        ):
            raise ValueError(
                "max_shuffle_partition_size must be a positive integer (at least 1)."
            )
        return self

    @property
    def _work_cols(self):
        return ["variantCount", "rank", "upperRank", "bucket"]

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.filter(f.col("studyType").isNotNull())

        w1 = Window.partitionBy("tagVariantId")
        w2 = Window.partitionBy("chromosome").orderBy(
            "variantCount", "tagVariantId", "studyLocusId"
        )

        result = (
            df.withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
            )
            .withColumn("variantCount", f.count(f.col("tagVariantId")).over(w1))
            .withColumn("rank", f.rank().over(w2))
            .withColumn("upperRank", f.col("rank") + f.col("variantCount") - 1)
            .withColumn(
                "bucket",
                f.ceil(f.col("upperRank") / f.lit(self.max_shuffle_partition_size)),
            )
            .withColumn(
                "partition", f.concat_ws("_", f.col("chromosome"), f.col("bucket"))
            )
            .withColumn("strategy", f.lit(PartitionStrategyRegistry.INPUT_BASED.value))
        )
        return result


class OutputBasedPartition(BaseModel):
    max_overlap_output_size: int = 100
    """The maximum expected output size of overlaps for each partition in bytes. If not provided, the partitioning will be done solely based on the input size."""

    @model_validator(mode="after")
    def validate_max_overlap_output_size(self):
        if self.max_overlap_output_size is None or self.max_overlap_output_size <= 1:
            raise ValueError(
                "max_overlap_output_size must be a positive integer (at least 1)."
            )
        return self

    @property
    def _work_cols(self):
        return [
            "variantCount",
            "rank",
            "rowNumberOverVariantId",
            "sparseVariantCount",
            "cumulativeOutputRowCount",
            "bucket",
        ]

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.filter(f.col("studyType").isNotNull())

        w1 = Window.partitionBy("tagVariantId").orderBy("studyLocusId")
        w2 = Window.partitionBy("chromosome").orderBy(
            "variantCount", "tagVariantId", "studyLocusId"
        )

        result = (
            df.withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
            )
            .withColumn("variantCount", f.count(f.col("tagVariantId")).over(w1))
            .withColumn("rank", f.rank().over(w2))
            .withColumn("rowNumberOverVariantId", f.row_number().over(w1))
            .withColumn(
                "sparseVariantCount",
                f.when(
                    f.col("rowNumberOverVariantId") == 1, f.col("variantCount")
                ).otherwise(0),
            )
            .withColumn(
                "cumulativeOutputRowCount",
                f.pow(f.sum(f.col("sparseVariantCount")).over(w2), 2) * 1 / 2,
            )
            .withColumn(
                "bucket",
                f.ceil(
                    f.col("cumulativeOutputRowCount")
                    / f.lit(self.max_overlap_output_size)
                ),
            )
            .withColumn(
                "partition", f.concat_ws("_", f.col("chromosome"), f.col("bucket"))
            )
            .withColumn("strategy", f.lit(PartitionStrategyRegistry.OUTPUT_BASED.value))
        )
        return result


class RegionCumulativeOverlapPartition(BaseModel):
    """Overlap partitioning using a genomic range join at locus level to produce exact overlap counts.

    Instead of a crossJoin (O(N_loci²)) or a variant-level equi-join (hot-variant skew), this
    strategy joins loci whose genomic intervals overlap — (l.start ≤ r.end) & (r.start ≤ l.end) &
    (l.chr = r.chr) — then uses array_intersect for exact per-pair overlap sizes. The resulting
    per-locus counts feed into the same cumulative bucket assignment as CumulativeOverlapBased2.
    """

    max_cumulative_overlap_output_size: int = 100
    """The maximum expected cumulative output size of overlaps for each partition. Controls bucket granularity — smaller values produce more, finer-grained partitions."""

    @model_validator(mode="after")
    def validate_max_cumulative_overlap_output_size(self):
        """Validate that max_cumulative_overlap_output_size is a positive integer greater than 1."""
        if (
            self.max_cumulative_overlap_output_size is None
            or self.max_cumulative_overlap_output_size <= 1
        ):
            raise ValueError(
                "max_cumulative_overlap_output_size must be a positive integer (at least 1)."
            )
        return self

    @property
    def _work_cols(self):
        return [
            "locusStart",
            "locusEnd",
            "variantIds",
            "variantCount",
            "locusOverlapCount",
            "rowsPerVariant",
            "rowNumberOverVariantId",
            "sparseRowsPerVariant",
            "cumulativeSparseRowsPerVariant",
            "bucket",
        ]

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.filter(f.col("studyType").isNotNull() & f.col("locus").isNotNull() & (f.size("locus") > 0))

        # Compact locus-level table: one row per locus with genomic bounds and a flat variant array.
        # Avoids exploding to variant level for the pre-pass.
        locus_bounds = df.select(
            "studyLocusId",
            "chromosome",
            f.array_min(
                f.transform("locus", lambda x: f.split(x.getField("variantId"), "_")[1].cast("long"))
            ).alias("locusStart"),
            f.array_max(
                f.transform("locus", lambda x: f.split(x.getField("variantId"), "_")[1].cast("long"))
            ).alias("locusEnd"),
            f.transform("locus", lambda x: x.getField("variantId")).alias("variantIds"),
        )

        # Range join at locus level: only pairs whose genomic intervals overlap are compared.
        # (l.start <= r.end) & (r.start <= l.end) & (l.chr = r.chr)
        # The range_join hint bins positions into 1 Mb windows so Spark only compares loci
        # within the same bin rather than doing a full SortMergeJoin per chromosome.
        # studyLocusId > condition de-duplicates pairs; array_intersect gives exact overlap size.
        candidate_pairs = (
            locus_bounds.alias("left")
            .join(
                locus_bounds.alias("right").hint("range_join", 1_000_000),
                on=(
                    (f.col("left.chromosome") == f.col("right.chromosome"))
                    & (f.col("left.locusStart") <= f.col("right.locusEnd"))
                    & (f.col("right.locusStart") <= f.col("left.locusEnd"))
                    & (f.col("left.studyLocusId") > f.col("right.studyLocusId"))
                ),
                how="inner",
            )
            .withColumn(
                "overlapSize",
                f.size(f.array_intersect("left.variantIds", "right.variantIds")),
            )
            .filter(f.col("overlapSize") > 0)
            .select(
                f.col("left.studyLocusId").alias("leftStudyLocusId"),
                f.col("right.studyLocusId").alias("rightStudyLocusId"),
            )
        )

        # Count overlapping partners per locus.
        # Union both sides so each locus accumulates counts from all its pairs.
        pre_overlap_cumulative = (
            candidate_pairs.select(f.col("leftStudyLocusId").alias("studyLocusId"))
            .union(candidate_pairs.select(f.col("rightStudyLocusId").alias("studyLocusId")))
            .groupBy("studyLocusId")
            .agg(f.count("*").alias("locusOverlapCount"))
        )

        # Explode to variant level and join with per-locus overlap counts.
        df_with_estimated = (
            df.withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
            )
            .join(pre_overlap_cumulative, on="studyLocusId", how="left")
            .fillna({"locusOverlapCount": 0})
        )

        # Reuse CumulativeOverlapBased2 bucket assignment logic unchanged.
        w1 = (
            Window.partitionBy("tagVariantId")
            .orderBy("studyLocusId", "locusOverlapCount")
            .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        w2 = Window.partitionBy("chromosome").orderBy(
            "tagVariantId", "studyLocusId", "locusOverlapCount"
        )
        w3 = Window.partitionBy("tagVariantId").orderBy(
            "studyLocusId", "locusOverlapCount"
        )

        return (
            df_with_estimated
            .withColumn("variantCount", f.count(f.col("tagVariantId")).over(w1))
            .withColumn(
                "rowsPerVariant",
                f.sum(f.col("locusOverlapCount")).over(w1)
                - (f.col("variantCount") * (f.col("variantCount") - 1) / 2),
            )
            .withColumn("rowNumberOverVariantId", f.row_number().over(w3))
            .withColumn(
                "sparseRowsPerVariant",
                f.when(
                    f.col("rowNumberOverVariantId") == 1, f.col("rowsPerVariant")
                ).otherwise(0),
            )
            .withColumn(
                "cumulativeSparseRowsPerVariant",
                f.sum(f.col("sparseRowsPerVariant")).over(w2),
            )
            .withColumn(
                "bucket",
                f.ceil(
                    f.col("cumulativeSparseRowsPerVariant")
                    / f.lit(self.max_cumulative_overlap_output_size)
                ),
            )
            .withColumn(
                "partition", f.concat_ws("_", f.col("chromosome"), f.col("bucket"))
            )
            .withColumn(
                "strategy",
                f.lit(PartitionStrategyRegistry.REGION_CUMULATIVE_OVERLAP_BASED.value),
            )
        )


class OverlapPartition:
    """Entry point for overlap partitioning. Instantiates the requested strategy and exposes a single partition() method."""

    def __init__(self, strategy: str, **kwargs) -> None:
        """Instantiate the requested partition strategy.

        Args:
            strategy (str): One of the PartitionStrategyRegistry values.
            **kwargs: Forwarded to the strategy's constructor (e.g. max_cumulative_overlap_output_size).
        """
        self.strategy = PartitionStrategyRegistry(strategy)
        match self.strategy:
            case PartitionStrategyRegistry.INPUT_BASED:
                self.implementer = InputBasedPartition(**kwargs)
            case PartitionStrategyRegistry.OUTPUT_BASED:
                self.implementer = OutputBasedPartition(**kwargs)
            case PartitionStrategyRegistry.CUMULATIVE_OVERLAP_BASED:
                self.implementer = CumulativeOverlapBasedPartition(**kwargs)
            case PartitionStrategyRegistry.CUMULATIVE_OVERLAP_BASED_2:
                self.implementer = CumulativeOverlapBasedPartition2(**kwargs)
            case PartitionStrategyRegistry.REGION_CUMULATIVE_OVERLAP_BASED:
                self.implementer = RegionCumulativeOverlapPartition(**kwargs)
            case _:
                raise UnknownPartitionStrategyError(
                    f"Unsupported partition strategy: {self.strategy}"
                )

    def partition(self, df: DataFrame) -> DataFrame:
        """Apply the partition strategy and return the exploded, bucketed locus DataFrame.

        Args:
            df (DataFrame): Raw StudyLocus DataFrame with unexploded locus arrays.

        Returns:
            DataFrame: Exploded locus rows with chromosome, partition, strategy, and tag variant stat columns.
        """
        return (
            self.implementer.transform(df)
            .repartition("chromosome", "bucket")
            .sortWithinPartitions("tagVariantId", "studyLocusId")
            .select(
                "chromosome",
                "studyLocusId",
                "studyId",
                "studyType",
                "partition",
                "strategy",
                "tagVariantId",
                "logBF",
                "posteriorProbability",
                "pValueMantissa",
                "pValueExponent",
                "beta",
            )
        )
