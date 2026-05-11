"""Benchmark L2G dataset."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.datasource.open_targets.l2g_gold_standard import (
    OpenTargetsL2GGoldStandard,
)

if False:  # pragma: no cover
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class L2GBenchmark(Dataset):
    """Benchmark table for L2G model development."""

    OTG_CURATION_SEED_SOURCE = "otg_curation_seed"

    @classmethod
    def get_schema(cls: type[L2GBenchmark]) -> StructType:
        """Return the benchmark schema.

        Returns:
            StructType: Schema for the benchmark dataset.
        """
        return parse_spark_schema("benchmark_l2g.json")

    @classmethod
    def from_otg_curation(
        cls: type[L2GBenchmark],
        gold_standard_curation: DataFrame,
        benchmark_version: str,
        release_version: str,
        pipeline_run_id: str,
    ) -> L2GBenchmark:
        """Create a seed-only benchmark from OTG L2G curation.

        Args:
            gold_standard_curation (DataFrame): Raw OTG gold-standard curation.
            benchmark_version (str): Benchmark version identifier.
            release_version (str): Data release version identifier.
            pipeline_run_id (str): Build run identifier.

        Returns:
            L2GBenchmark: Seed-only benchmark dataset.
        """
        positive_set = OpenTargetsL2GGoldStandard.parse_positive_curation(
            gold_standard_curation
        )

        benchmark_df = positive_set.select(
            f.lit(benchmark_version).alias("benchmarkVersion"),
            f.lit(release_version).alias("releaseVersion"),
            f.lit(pipeline_run_id).alias("pipelineRunId"),
            "studyLocusId",
            "studyId",
            "variantId",
            "geneId",
            f.lit(None).cast("string").alias("traitFromSourceMappedId"),
            f.lit("positive").alias("goldStandardSet"),
            f.lit(True).alias("trainingInclusion"),
            f.lit(True).alias("isSeedPositive"),
            f.concat_ws("|", f.sort_array("sources")).alias("seedSourceSummary"),
            f.size("sources").alias("seedSourceCount"),
            f.lit(cls.OTG_CURATION_SEED_SOURCE).alias("candidateGeneSource"),
            f.lit(None).cast("string").alias("exclusionReasonSummary"),
            "sources",
        )

        return cls(_df=benchmark_df, _schema=cls.get_schema())
