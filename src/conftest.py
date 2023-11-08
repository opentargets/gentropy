"""Test configuration within src dir (doctests)."""
from __future__ import annotations

from typing import Any

import pytest
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_testing_conf() -> SparkConf:
    """Get SparkConf for testing purposes.

    Returns:
        SparkConf: SparkConf with settings for testing.
    """
    return (
        SparkConf()
        .set("spark.driver.bindAddress", "127.0.0.1")
        # No shuffling.
        .set("spark.sql.shuffle.partitions", "1")
        # UI settings.
        .set("spark.ui.showConsoleProgress", "false")
        .set("spark.ui.enabled", "false")
        .set("spark.ui.dagGraph.retainedRootRDDs", "1")
        .set("spark.ui.retainedJobs", "1")
        .set("spark.ui.retainedStages", "1")
        .set("spark.ui.retainedTasks", "1")
        .set("spark.sql.ui.retainedExecutions", "1")
        .set("spark.worker.ui.retainedExecutors", "1")
        .set("spark.worker.ui.retainedDrivers", "1")
        # Fixed memory.
        .set("spark.driver.memory", "2g")
    )


@pytest.fixture(scope="session", autouse=True)
def spark(
    doctest_namespace: dict[str, Any], tmp_path_factory: pytest.TempPathFactory
) -> SparkSession:
    """Local spark session for testing purposes.

    It returns a session and make it available to doctests through
    the `spark` namespace.

    Args:
        doctest_namespace (dict[str, Any]): pytest namespace for doctests
        tmp_path_factory (pytest.TempPathFactory): pytest tmp_path_factory

    Returns:
        SparkSession: local spark session
    """
    spark = (
        SparkSession.builder.config(conf=get_spark_testing_conf())
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )
    doctest_namespace["spark"] = spark
    return spark
