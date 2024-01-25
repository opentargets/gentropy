"""Testing creating dataset docs."""
from typing import Any

import pytest
from gentropy.dataset.summary_statistics import SummaryStatistics

from docs.src_snippets.howto.python_api.b_create_dataset import (
    create_from_pandas,
    create_from_parquet,
    create_from_source,
)


@pytest.mark.parametrize(
    "func",
    [
        create_from_parquet,
        create_from_source,
        create_from_pandas,
    ],
)
def test_create_dataset(func: Any) -> None:
    """Test any method in create_dataset returns an instance of SummaryStatistics."""
    assert isinstance(func(), SummaryStatistics)
