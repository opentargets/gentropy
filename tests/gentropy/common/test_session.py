"""Tests Gentropy session."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gentropy.common.session import Log4j, Session

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from pyspark.sql import SparkSession


def test_session_creation() -> None:
    """Test sessio creation with mock data."""
    assert isinstance(Session(spark_uri="local[1]"), Session)


def test_hail_configuration(hail_home: str) -> None:
    """Assert that Hail configuration is set when start_hail is True."""
    session = Session(spark_uri="local[1]", hail_home=hail_home, start_hail=True)

    expected_hail_conf = {
        "spark.jars": f"{hail_home}/backend/hail-all-spark.jar",
        "spark.driver.extraClassPath": f"{hail_home}/backend/hail-all-spark.jar",
        "spark.executor.extraClassPath": "./hail-all-spark.jar",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
    }

    observed_conf = dict(session.spark.sparkContext.getConf().getAll())
    # sourcery skip: no-loop-in-tests
    for key, value in expected_hail_conf.items():
        assert observed_conf.get(key) == value, f"Expected {key} to be set to {value}"


def test_log4j_creation(spark: SparkSession) -> None:
    """Test session log4j."""
    assert isinstance(Log4j(spark=spark), Log4j)


@pytest.fixture(scope="function")
def mock_data_files(tmp_path: Path) -> Generator[Path, None, None]:
    """Create mock data files for testing."""
    tmp_files = [
        tmp_path / "test_path.parquet",
        tmp_path / "test_path.csv",
        tmp_path / "test_path.tsv",
        tmp_path / "test_path.json",
        tmp_path / "test_path.txt",
        tmp_path / "test_path.unknown",
    ]
    tmp_dir = tmp_path / "test_path"
    tmp_dir.mkdir(exist_ok=True)

    tmp_files_nested = [tmp_dir / "part.1.parquet", tmp_dir / "part.2.parquet"]
    [f.touch(exist_ok=True) for f in tmp_files]
    [f.touch(exist_ok=True) for f in tmp_files_nested]

    yield tmp_dir

    [f.unlink(missing_ok=True) for f in tmp_files if f.exists()]
    [f.unlink(missing_ok=True) for f in tmp_files_nested if f.exists()]
    tmp_dir.rmdir()


@pytest.mark.skip(reason="Not implemented yet.")
@pytest.mark.parametrize(
    ["path", "format", "kwargs"],
    [
        pytest.param("test_path.parquet", None, {}, id="implicit parquet"),
        pytest.param("test_path.csv", None, {}, id="implicit csv"),
        pytest.param("test_path.tsv", None, {}, id="implicit tsv"),
        pytest.param("test_path.json", None, {}, id="implicit json"),
        pytest.param("test_path.txt", None, {}, id="implicit txt fallback to tsv"),
        pytest.param("test_path.unknown", None, {}, id="unknown fallback to parquet"),
        pytest.param("test_path/", None, {}, id="dataset fallback to parquet"),
        pytest.param("test_path.parquet", "parquet", {}, id="explicit parquet"),
        pytest.param("test_path.csv", "csv", {"header": "true"}, id="explicit csv"),
        pytest.param(
            "test_path.tsv", "csv", {"header": "true", "sep": "\t"}, id="explicit tsv"
        ),
        pytest.param("test_path.json", "json", {}, id="explicit json"),
        pytest.param(
            ["test_path/part.1.parquet", "test_path/part.2.parquet"],
            "parquet",
            {},
            id="2 explicit parquet files",
        ),
    ],
)
def test_load_data(path: str, format: str, kwargs: dict, mock_data_files: Path) -> None:
    """Test Session.load_data method."""
    pass


@pytest.mark.skip(reason="Not implemented yet.")
@pytest.mark.parametrize(
    ["url", "format", "error"],
    [
        pytest.param(
            "https://some_example.com/data.parquet",
            "parquet",
            "Only 'csv' format is supported for loading data from URL.",
            id="unsupported format parquet",
        ),
        pytest.param(
            "https://some_example.com/data.json",
            "json",
            "Only 'csv' format is supported for loading data from URL.",
            id="unsupported format json",
        ),
        pytest.param(
            "http://some_example.com/data.csv",
            "csv",
            "",
            id="supported format csv",
        ),
        pytest.param(
            [
                "http://some_example.com/data1.csv",
                "http://some_example.com/data2.csv",
            ],
            "csv",
            "Reading multiple files over HTTP/HTTPS is not supported.",
            id="unsupported multiple URLs",
        ),
    ],
)
def test_load_from_url(url: str, format: str, error: str) -> None:
    """Test Session.load_data method with URL input."""
    pass
