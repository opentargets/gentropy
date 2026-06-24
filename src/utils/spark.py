"""Spark utilities."""

from __future__ import annotations

from pathlib import Path

from pyspark.conf import SparkConf


def get_spark_testing_conf(with_hail: bool = False) -> SparkConf:
    """Get SparkConf for testing purposes.

    Args:
        with_hail (bool): If True, include hail-specific keys (jar path,
            classpath entries, Kryo serializer/registrator). Hail is imported
            lazily inside this branch so callers without the ``[hail]`` extra
            can still build a hail-free testing conf.

    Returns:
        SparkConf: SparkConf with settings for testing.
    """
    conf = (
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

    if with_hail:
        try:
            import hail as hl
        except ImportError as exc:
            from gentropy.common.imports import install_hint

            raise ImportError(install_hint("hail")) from exc

        hail_home = Path(hl.__file__).parent.as_posix()
        conf = (
            conf.set("spark.jars", f"{hail_home}/backend/hail-all-spark.jar")
            .set(
                "spark.driver.extraClassPath",
                f"{hail_home}/backend/hail-all-spark.jar",
            )
            .set("spark.executor.extraClassPath", "./hail-all-spark.jar")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
        )

    return conf
