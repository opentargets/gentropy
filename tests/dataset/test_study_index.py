"""Test study index dataset."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from otg.dataset.study_index import StudyIndex


def test_study_index_creation(mock_study_index: StudyIndex) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index, StudyIndex)


def test_study_index_type_lut(mock_study_index: StudyIndex) -> None:
    """Test study index type lut."""
    assert isinstance(mock_study_index.study_type_lut(), DataFrame)


def test_aggregate_and_map_ancestries__correctness(spark: SparkSession) -> None:
    """Test if population are mapped and relative sample sizes are calculated."""
    data = [
        (
            "s1",
            "East Asian",
            100,
        ),
        (
            "s1",
            "Finnish",
            100,
        ),
        (
            "s1",
            "NR",
            100,
        ),
        (
            "s1",
            "European",
            100,
        ),
    ]

    columns = ["studyId", "ancestry", "sampleSize"]

    df = (
        spark.createDataFrame(data, columns)
        .groupBy("studyId")
        .agg(
            f.collect_list(f.struct("ancestry", "sampleSize")).alias("discoverySamples")
        )
        .select(
            StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")).alias(
                "parsedPopulation"
            )
        )
    )

    # Asserting that there are three population (both NR and Europeans are grounded to 'nfe'):
    assert (df.select(f.explode("parsedPopulation")).count()) == 3

    # Asserting that the relative count go to 1.0
    assert (
        df.select(
            f.aggregate(
                "parsedPopulation", f.lit(0.0), lambda y, x: y + x.relativeSampleSize
            ).alias("sum")
        ).collect()[0]["sum"]
    ) == 1.0


def test_aggregate_samples_by_ancestry__correctness(spark: SparkSession) -> None:
    """Test correctness of the ancestry aggregator function."""
    data = [
        (
            "s1",
            "a1",
            100,
        ),
        (
            "s1",
            "a1",
            100,
        ),
        (
            "s1",
            "a2",
            100,
        ),
    ]

    columns = ["studyId", "ancestry", "sampleSize"]

    df = (
        spark.createDataFrame(data, columns)
        .groupBy("studyId")
        .agg(
            f.collect_list(f.struct("ancestry", "sampleSize")).alias("discoverySamples")
        )
        .select(
            f.aggregate(
                "discoverySamples",
                f.array_distinct(
                    f.transform(
                        "discoverySamples",
                        lambda x: f.struct(
                            x.ancestry.alias("ancestry"), f.lit(0.0).alias("sampleSize")
                        ),
                    )
                ),
                StudyIndex._aggregate_samples_by_ancestry,
            ).alias("test_output")
        )
        .persist()
    )

    # Asserting the number of aggregated population:
    assert (
        df.filter(f.col("studyId") == "s1").select(f.explode("test_output")).count()
    ) == 2

    # Asserting the number of aggregated sample size:
    assert (
        df.filter(f.col("studyId") == "s1")
        .select(
            f.aggregate("test_output", f.lit(0.0), lambda y, x: x.sampleSize + y).alias(
                "totalSamples"
            )
        )
        .collect()[0]["totalSamples"]
    ) == 300.0
