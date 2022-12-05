"""Test configuration within src dir (doctests)."""
from __future__ import annotations

import subprocess
from typing import Any

import hail as hl
import pytest
from pyspark import SparkConf
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
    # configure spark using hail backend
    hail_home = subprocess.check_output(
        "poetry run pip show hail | grep Location | awk -F' ' '{print $2 \"/hail\"}'",
        shell=True,
        text=True,
    ).strip()
    conf = (
        SparkConf()
        .set(
            "spark.jars",
            hail_home + "/backend/hail-all-spark.jar",
        )
        .set(
            "spark.driver.extraClassPath",
            hail_home + "/backend/hail-all-spark.jar",
        )
        .set("spark.executor.extraClassPath", "./hail-all-spark.jar")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
        # .set("spark.kryoserializer.buffer", "512m")
    )
    # init spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    doctest_namespace["spark"] = spark
    # init hail session
    hl.init(sc=spark.sparkContext)
    return spark
