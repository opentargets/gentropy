"""Benchmark L2G build report dataset."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.l2g_benchmark import L2GBenchmark

if False:  # pragma: no cover
    from pyspark.sql.types import StructType


@dataclass
class L2GBenchmarkBuildReport(Dataset):
    """Summary report for a benchmark build."""

    @classmethod
    def get_schema(cls: type[L2GBenchmarkBuildReport]) -> StructType:
        """Return the build report schema.

        Returns:
            StructType: Schema for the benchmark build report dataset.
        """
        return parse_spark_schema("benchmark_l2g_build_report.json")

    @classmethod
    def from_benchmark(
        cls: type[L2GBenchmarkBuildReport],
        benchmark: L2GBenchmark,
    ) -> L2GBenchmarkBuildReport:
        """Build a summary report from a benchmark dataset.

        Args:
            benchmark (L2GBenchmark): Benchmark dataset to summarize.

        Returns:
            L2GBenchmarkBuildReport: Summary report with basic coverage metrics.
        """
        summary_df = benchmark.df.agg(
            f.first("benchmarkVersion").alias("benchmarkVersion"),
            f.first("releaseVersion").alias("releaseVersion"),
            f.first("pipelineRunId").alias("pipelineRunId"),
            f.count("*").alias("rowCount"),
            f.sum(
                f.when(f.col("isSeedPositive"), f.lit(1)).otherwise(f.lit(0))
            ).alias("seedPositiveCount"),
            f.sum(
                f.when(f.col("goldStandardSet") == "positive", f.lit(1)).otherwise(
                    f.lit(0)
                )
            ).alias("finalPositiveCount"),
            f.sum(
                f.when(f.col("goldStandardSet") == "negative", f.lit(1)).otherwise(
                    f.lit(0)
                )
            ).alias("finalNegativeCount"),
            f.sum(
                f.when(f.col("goldStandardSet") == "excluded", f.lit(1)).otherwise(
                    f.lit(0)
                )
            ).alias("excludedCount"),
            f.sum(
                f.when(f.col("trainingInclusion"), f.lit(1)).otherwise(f.lit(0))
            ).alias("trainingIncludedCount"),
            f.countDistinct("studyLocusId").alias("distinctStudyLocusCount"),
            f.countDistinct("geneId").alias("distinctGeneCount"),
        )

        return cls(_df=summary_df, _schema=cls.get_schema())
