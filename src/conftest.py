"""Test configuration within src dir (doctests)."""
from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def spark(doctest_namespace: dict[str, Any]) -> SparkSession:
    """Local spark session for testing purposes.

    It returns a session and make it available to doctests through
    the `spark` namespace.

    Args:
        doctest_namespace (Dict[str, Any]): pytest namespace for doctests

    Returns:
        SparkSession: local spark session
    """
    # init spark session
    spark = (
        SparkSession.builder.master("local[1]")
        # no shuffling
        .config("spark.sql.shuffle.partitions", "1")
        # ui settings
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        # fixed memory
        .config("spark.driver.memory", "2g")
        .appName("test")
        .getOrCreate()
    )

    doctest_namespace["spark"] = spark
    return spark
