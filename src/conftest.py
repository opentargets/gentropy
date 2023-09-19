"""Test configuration within src dir (doctests)."""
from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession

from src.utils.spark import get_spark_testing_conf


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
    spark = (
        SparkSession.builder.config(conf=get_spark_testing_conf())
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )
    doctest_namespace["spark"] = spark
    return spark
