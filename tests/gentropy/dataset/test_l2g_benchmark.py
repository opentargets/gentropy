"""Tests for the L2G benchmark datasets."""

from __future__ import annotations

from pyspark.sql import functions as f

from gentropy.dataset.l2g_benchmark import L2GBenchmark
from gentropy.dataset.l2g_benchmark_build_report import L2GBenchmarkBuildReport
from gentropy.datasource.open_targets.l2g_gold_standard import (
    OpenTargetsL2GGoldStandard,
)


def test_from_otg_curation(sample_l2g_gold_standard) -> None:
    """Benchmark rows should mirror the parsed OTG positive set."""
    benchmark = L2GBenchmark.from_otg_curation(
        gold_standard_curation=sample_l2g_gold_standard,
        benchmark_version="ot_26_03_v1",
        release_version="26.03",
        pipeline_run_id="run-001",
    )
    positive_set = OpenTargetsL2GGoldStandard.parse_positive_curation(
        sample_l2g_gold_standard
    )

    assert benchmark.df.count() == positive_set.count()
    assert benchmark.df.filter(f.col("goldStandardSet") != "positive").count() == 0
    assert benchmark.df.filter(~f.col("trainingInclusion")).count() == 0
    assert benchmark.df.filter(~f.col("isSeedPositive")).count() == 0
    assert set(benchmark.df.columns) == {
        "benchmarkVersion",
        "releaseVersion",
        "pipelineRunId",
        "studyLocusId",
        "studyId",
        "variantId",
        "geneId",
        "traitFromSourceMappedId",
        "goldStandardSet",
        "trainingInclusion",
        "isSeedPositive",
        "seedSourceSummary",
        "seedSourceCount",
        "candidateGeneSource",
        "exclusionReasonSummary",
        "sources",
    }


def test_build_report_from_benchmark(spark) -> None:
    """Build report should summarize visible benchmark counts."""
    benchmark = L2GBenchmark(
        _df=spark.createDataFrame(
            [
                (
                    "ot_26_03_v1",
                    "26.03",
                    "run-001",
                    "sl1",
                    "study1",
                    "variant1",
                    "gene1",
                    None,
                    "positive",
                    True,
                    True,
                    "otg_curation",
                    1,
                    "otg_curation_seed",
                    None,
                    ["otg_curation"],
                ),
                (
                    "ot_26_03_v1",
                    "26.03",
                    "run-001",
                    "sl2",
                    "study2",
                    "variant2",
                    "gene2",
                    None,
                    "positive",
                    True,
                    True,
                    "otg_curation",
                    1,
                    "otg_curation_seed",
                    None,
                    ["otg_curation"],
                ),
            ],
            schema=L2GBenchmark.get_schema(),
        ),
        _schema=L2GBenchmark.get_schema(),
    )

    report = L2GBenchmarkBuildReport.from_benchmark(benchmark)
    row = report.df.collect()[0]

    assert row["benchmarkVersion"] == "ot_26_03_v1"
    assert row["releaseVersion"] == "26.03"
    assert row["pipelineRunId"] == "run-001"
    assert row["rowCount"] == 2
    assert row["seedPositiveCount"] == 2
    assert row["finalPositiveCount"] == 2
    assert row["finalNegativeCount"] == 0
    assert row["excludedCount"] == 0
    assert row["trainingIncludedCount"] == 2
    assert row["distinctStudyLocusCount"] == 2
    assert row["distinctGeneCount"] == 2
    assert report.df.columns == L2GBenchmarkBuildReport.get_schema().fieldNames()
