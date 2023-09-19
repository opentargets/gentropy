"""Test configuration within src dir (doctests)."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from otg.common.session import Session
from src.utils.spark import get_spark_testing_conf

if TYPE_CHECKING:
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
    spark = Session(
        spark_uri="local[1]", app_name="test", extended_conf=get_spark_testing_conf()
    ).spark

    doctest_namespace["spark"] = spark
    return spark
