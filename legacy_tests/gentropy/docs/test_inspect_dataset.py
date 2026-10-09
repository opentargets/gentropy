"""Testing inspecting dataset docs."""

from pyspark.sql.types import StructType

from docs.src_snippets.howto.python_api.d_inspect_dataset import (
    filter_dataset,
    get_dataset_schema,
    interact_w_dataframe,
)
from gentropy.dataset.summary_statistics import SummaryStatistics


def test_filter_dataset(mock_summary_statistics: SummaryStatistics) -> None:
    """Test filter_dataset returns a SummaryStatistics."""
    assert isinstance(filter_dataset(mock_summary_statistics), SummaryStatistics)


def test_interact_w_dataframe(mock_summary_statistics: SummaryStatistics) -> None:
    """Test interact_w_dataframe returns a SummaryStatistics."""
    assert isinstance(interact_w_dataframe(mock_summary_statistics), SummaryStatistics)


def test_get_dataset_schema(mock_summary_statistics: SummaryStatistics) -> None:
    """Test get_dataset_schema returns a StructType."""
    assert isinstance(get_dataset_schema(mock_summary_statistics), StructType)
