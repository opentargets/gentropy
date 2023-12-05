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


class TestSetPartitions:
    """Test TestDataset.set_partitions."""

    @pytest.mark.parametrize("new_partitions_expr", (+1, -1))
    def test_set_partitions(self: TestSetPartitions, new_partitions_expr: int) -> None:
        """Test Dataset.set_partitions."""
        initial_partitions = self.test_dataset._df.rdd.getNumPartitions()
        new_partitions = (
            initial_partitions + new_partitions_expr if initial_partitions > 1 else 1
        )
        self.test_dataset.set_partitions(new_partitions)
        assert self.test_dataset._df.rdd.getNumPartitions() == new_partitions

    @pytest.fixture(autouse=True)
    def _setup(self: TestSetPartitions, spark: SparkSession) -> None:
        """Setup fixture."""
        self.test_dataset = TestDataset(
            _df=spark.createDataFrame(
                [(1,), (2,), (3,)], schema=TestDataset.get_schema()
            ),
            _schema=TestDataset.get_schema(),
        )
