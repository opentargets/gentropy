"""Test Dataset class."""
from __future__ import annotations

import pyspark.sql.functions as f
import pytest
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_index import StudyIndex
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


class MockDataset(Dataset):
    """Concrete subclass of Dataset for testing. Necessary because Dataset is abstract."""

    @classmethod
    def get_schema(cls) -> StructType:
        """Get schema."""
        return StructType([StructField("value", IntegerType(), False)])


class TestCoalesceAndRepartition:
    """Test TestDataset.coalesce and TestDataset.repartition."""

    def test_repartition(self: TestCoalesceAndRepartition) -> None:
        """Test Dataset.repartition."""
        initial_partitions = self.test_dataset._df.rdd.getNumPartitions()
        new_partitions = initial_partitions + 1
        self.test_dataset.repartition(new_partitions)
        assert self.test_dataset._df.rdd.getNumPartitions() == new_partitions

    def test_coalesce(self: TestCoalesceAndRepartition) -> None:
        """Test Dataset.coalesce."""
        initial_partitions = self.test_dataset._df.rdd.getNumPartitions()
        new_partitions = initial_partitions - 1 if initial_partitions > 1 else 1
        self.test_dataset.coalesce(new_partitions)
        assert self.test_dataset._df.rdd.getNumPartitions() == new_partitions

    @pytest.fixture(autouse=True)
    def _setup(self: TestCoalesceAndRepartition, spark: SparkSession) -> None:
        """Setup fixture."""
        self.test_dataset = MockDataset(
            _df=spark.createDataFrame(
                [(1,), (2,), (3,)], schema=MockDataset.get_schema()
            ),
            _schema=MockDataset.get_schema(),
        )


def test_dataset_filter(mock_study_index: StudyIndex) -> None:
    """Test Dataset.filter."""
    expected_filter_value = "gwas"
    condition = f.col("studyType") == expected_filter_value

    filtered = mock_study_index.filter(condition)
    assert (
        filtered.df.select("studyType").distinct().toPandas()["studyType"].to_list()[0]
        == expected_filter_value
    ), "Filtering failed."
