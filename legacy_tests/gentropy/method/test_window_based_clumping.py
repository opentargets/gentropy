"""Test window-based clumping."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.ml import functions as fml
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from gentropy.dataset.summary_statistics import SummaryStatistics


def test_window_based_clump__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    assert isinstance(
        WindowBasedClumping.clump(mock_summary_statistics, distance=250_000),
        StudyLocus,
    )
    assert isinstance(
        WindowBasedClumping.clump(
            mock_summary_statistics,
            distance=250_000,
        ),
        StudyLocus,
    )


def test_window_based_clump__correctness(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    clumped = sample_summary_statistics.window_based_clumping(250_000)

    # One semi index was found:
    assert clumped.df.count() == 1

    # Assert the variant found:
    assert (clumped.df.filter(f.col("variantId") == "18_12843138_T_C").count()) == 1


def test_window_based_clump_with_locus__correctness(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test window-based clumping."""
    clumped = sample_summary_statistics.window_based_clumping(
        distance=250_000,
    )
    clumped = clumped.annotate_locus_statistics(
        sample_summary_statistics, collect_locus_distance=250_000
    )

    # Asserting the presence of locus key:
    assert "locus" in clumped.df.columns

    # One semi index was found:
    assert clumped.df.count() == 1

    # Assert the variant found:
    assert (clumped.df.filter(f.col("variantId") == "18_12843138_T_C").count()) == 1

    # Assert the number of variants in the locus:
    assert (clumped.df.select(f.explode_outer("locus").alias("loci")).count()) == 218


def test_prune_peak(spark: SparkSession) -> None:
    """Test the pruning of peaks.

    Args:
        spark (SparkSession): Spark session
    """
    data = [
        ("c", 3, 4.0),
        ("c", 4, 2.0),
        ("c", 6, 1.0),
        ("c", 8, 2.5),
        ("c", 9, 3.0),
    ]
    df = (
        spark.createDataFrame(data, ["cluster", "position", "negLogPValue"])
        .withColumn(
            "collected_positions",
            f.collect_list(f.col("position")).over(
                Window.partitionBy("cluster")
                .orderBy(f.col("negLogPValue").desc())
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        .withColumn(
            "isLeadList",
            f.udf(WindowBasedClumping._prune_peak, VectorUDT())(
                fml.array_to_vector(f.col("collected_positions")), f.lit(2)
            ),
        )
    )

    expected_result = spark.createDataFrame(
        [("c", [1.0, 1.0, 0.0, 0.0, 1.0])], ["cluster", "isLeadList"]
    ).withColumn("isLeadList", fml.array_to_vector(f.col("isLeadList")))

    # Compare the actual DataFrame with the expected result
    assert (
        df.select("cluster", "isLeadList").distinct().collect()
        == expected_result.collect()
    )
