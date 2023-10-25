"""Spark utilities."""
from __future__ import annotations

from pyspark.conf import SparkConf


def get_spark_testing_conf() -> SparkConf:
    """Get SparkConf for testing purposes."""
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
