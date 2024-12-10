"""Testing creating dataset docs."""

from typing import Any

import pytest

from docs.src_snippets.howto.python_api.b_create_dataset import (
    create_from_pandas,
    create_from_parquet,
    create_from_source,
)
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics


@pytest.mark.parametrize(
    "func",
    [
        create_from_parquet,
        create_from_source,
        create_from_pandas,
    ],
)
def test_create_dataset(func: Any, session: Session) -> None:
    """Test any method in create_dataset returns an instance of SummaryStatistics."""
    tested_func = func(session) if func != create_from_pandas else func()
    assert isinstance(tested_func, SummaryStatistics)
