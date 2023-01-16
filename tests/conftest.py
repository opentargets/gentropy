"""Unit test configuration."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local spark session for testing purposes.

    Returns:
        SparkSession: local spark session
    """
    return (
        SparkSession.builder.config("spark.driver.bindAddress", "127.0.0.1")
        .master("local")
        .appName("test")
        .getOrCreate()
    )
