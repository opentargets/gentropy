"""Test configuration within src dir (doctests)."""
from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession

from utils.spark import get_spark_testing_conf


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
    # Restart new session:
    spark = (
        SparkSession.builder.config(conf=get_spark_testing_conf())
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )
    doctest_namespace["spark"] = spark
    return spark
