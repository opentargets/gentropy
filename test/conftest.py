"""Common test fixtures."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from utils.spark import get_spark_testing_conf


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Local spark session for testing purposes."""
    spark = (
        SparkSession.Builder()
        .config(conf=get_spark_testing_conf())
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )
    yield spark

    spark.stop()
