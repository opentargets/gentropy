"""Tests Gentropy session."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from gentropy.common.session import Log4j, Session

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path
    from typing import Any


def test_list_hadoop_paths(session: Session, tmp_path: Path) -> None:
    """Test Session.list_hadoop_paths with the local Hadoop filesystem.

    Uses tmp_path (local files) so no GCS/S3/HDFS connection is needed —
    Hadoop's LocalFileSystem handles file:// paths identically to remote schemes.
    """
    (tmp_path / "a.tsv").write_text("col\nval\n")
    (tmp_path / "b.tsv").write_text("col\nval\n")
    (tmp_path / "c.parquet").write_bytes(b"PAR1")  # must not appear in *.tsv results

    matched = session.list_hadoop_paths((tmp_path / "*.tsv").as_posix())

    # Hadoop returns fully-qualified file:// URIs; compare by filename only
    assert len(matched) == 2
    assert {p.rsplit("/", 1)[-1] for p in matched} == {"a.tsv", "b.tsv"}
    assert all(p.endswith(".tsv") for p in matched)
    assert matched == sorted(matched)  # result must be sorted


def test_list_hadoop_paths_empty(session: Session, tmp_path: Path) -> None:
    """Test that list_hadoop_paths returns an empty list when no files match."""
    assert session.list_hadoop_paths((tmp_path / "*.nonexistent").as_posix()) == []


def test_log4j_creation(spark: SparkSession) -> None:
    """Test session log4j."""
    assert isinstance(Log4j(spark=spark), Log4j)


@pytest.fixture(scope="function")
def mock_data_files(tmp_path: Path) -> Generator[Path, None, None]:
    """Create mock data files for testing."""
    data = pd.DataFrame(
        {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }
    )
    tmp_files = [
        tmp_path / "test_path.parquet",
        tmp_path / "test_path.csv",
        tmp_path / "test_path.tsv",
        tmp_path / "test_path.json",
    ]
    tmp_dir = tmp_path / "test_path"
    tmp_dir.mkdir(exist_ok=True)

    data.to_parquet(tmp_files[0])
    data.to_csv(tmp_files[1], index=False)
    data.to_csv(tmp_files[2], index=False, sep="\t")
    data.to_json(tmp_files[3], orient="records", lines=True)

    tmp_files_nested = [tmp_dir / "part.1.parquet", tmp_dir / "part.2.parquet"]
    for d in tmp_files_nested:
        d.parent.mkdir(exist_ok=True)
        data.to_parquet(d)

    yield tmp_path

    for f in tmp_files + tmp_files_nested:
        f.unlink(missing_ok=True)
    tmp_dir.rmdir()


@pytest.mark.parametrize(
    ["path", "fmt", "kwargs"],
    [
        pytest.param("test_path.parquet", "parquet", {}, id="parquet"),
        pytest.param("test_path.csv", "csv", {}, id="csv"),
        pytest.param("test_path.tsv", "tsv", {}, id="tsv"),
        pytest.param("test_path.json", "json", {}, id="json"),
        pytest.param("test_path/", "parquet", {}, id="dataset fallback to parquet"),
    ],
)
def test_load_data(
    session: Session,
    path: str,
    fmt: str,
    kwargs: dict[str, str],
    mock_data_files: Path,
) -> None:
    """Test Session.load_data method."""
    full_path = mock_data_files / path
    try:
        session.load_data(full_path.as_posix(), fmt=fmt, **kwargs)
    except PySparkException as e:
        pytest.fail(f"Session.load_data raised an exception: {e}")


@pytest.mark.parametrize(
    ["url", "fmt", "error"],
    [
        pytest.param(
            "https://some_example.com/data.parquet",
            "parquet",
            "Only csv, tsv and json are URL supported formats",
            id="unsupported format parquet",
        ),
        pytest.param(
            "https://some_example.com/data.json",
            "json",
            None,
            id="supported format json",
        ),
        pytest.param(
            "http://some_example.com/data.csv",
            "csv",
            None,
            id="supported format csv",
        ),
    ],
)
def test_load_from_url(url: str, fmt: str, error: str, session: Session) -> None:
    """Test Session.load_data method with URL input."""

    def mock_read(*args: Any, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame(
            [
                ("val1", "val2"),
            ],
            columns=["col1", "col2"],
        )

    with pytest.MonkeyPatch.context() as m:
        m.setattr("gentropy.common.session.pd.read_csv", mock_read)
        m.setattr("gentropy.common.session.pd.read_json", mock_read)
        if error:
            with pytest.raises(ValueError, match=error):
                session.load_data(url, fmt=fmt)
        else:
            df = session.load_data(url, fmt=fmt)
            assert df.count() == 1


def test_load_csv_from_url_with_schema_keeps_ids_as_text(session: Session) -> None:
    """Integer ids with missing values stay whole numbers, and NA becomes null."""
    text = (
        "dataset_id\tsample_size\tpmid\nQTD000001\t84\t29379200\nQTD000002\t120\tNA\n"
    )
    read_csv = pd.read_csv

    def mock_read(url: str, **kwargs: Any) -> pd.DataFrame:
        """Read the test text instead of the URL."""
        return read_csv(io.StringIO(text), **kwargs)

    with pytest.MonkeyPatch.context() as m:
        m.setattr("gentropy.common.session.pd.read_csv", mock_read)
        df = session.load_data(
            "https://some_example.com/data.tsv",
            fmt="tsv",
            schema="dataset_id STRING, sample_size INT, pmid STRING",
        )
    rows = [row.asDict() for row in df.orderBy("dataset_id").collect()]
    assert rows == [
        {"dataset_id": "QTD000001", "sample_size": 84, "pmid": "29379200"},
        {"dataset_id": "QTD000002", "sample_size": 120, "pmid": None},
    ]


def test_load_csv_from_url_with_schema_fails_on_bad_values(session: Session) -> None:
    """A value that does not fit its schema type raises instead of becoming null."""
    text = "dataset_id\tsample_size\nQTD000001\tmany\n"
    read_csv = pd.read_csv

    def mock_read(url: str, **kwargs: Any) -> pd.DataFrame:
        """Read the test text instead of the URL."""
        return read_csv(io.StringIO(text), **kwargs)

    with pytest.MonkeyPatch.context() as m:
        m.setattr("gentropy.common.session.pd.read_csv", mock_read)
        with pytest.raises(ValueError, match="sample_size"):
            session.load_data(
                "https://some_example.com/data.tsv",
                fmt="tsv",
                schema="dataset_id STRING, sample_size INT",
            )
