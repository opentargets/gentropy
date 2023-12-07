"""Test Dataset class."""
from __future__ import annotations

from dataclasses import dataclass

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from otg.dataset.dataset import Dataset


@dataclass
class TestDataset(Dataset):
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
        self.test_dataset = TestDataset(
            _df=spark.createDataFrame(
                [(1,), (2,), (3,)], schema=TestDataset.get_schema()
            ),
            _schema=TestDataset.get_schema(),
        )
