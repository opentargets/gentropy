"""Tests Gentropy session."""

from __future__ import annotations

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
