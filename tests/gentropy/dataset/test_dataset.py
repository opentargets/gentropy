"""Test Dataset class."""

from __future__ import annotations

import numpy as np
import pyspark.sql.functions as f
import pytest
from pyspark.sql import Column, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_index import StudyIndex


class MockDataset(Dataset):
    """Concrete subclass of Dataset for testing. Necessary because Dataset is abstract."""

    @classmethod
    def get_schema(cls) -> StructType:
        """Get schema."""
        return StructType([StructField("value", IntegerType(), False)])


class TestDataset:
    """Test TestDataset.coalesce and TestDataset.repartition."""

    def test_repartition(self: TestDataset) -> None:
        """Test Dataset.repartition."""
        initial_partitions = self.test_dataset._df.rdd.getNumPartitions()
        new_partitions = initial_partitions + 1
        self.test_dataset.repartition(new_partitions)
        assert self.test_dataset._df.rdd.getNumPartitions() == new_partitions

    def test_coalesce(self: TestDataset) -> None:
        """Test Dataset.coalesce."""
        initial_partitions = self.test_dataset._df.rdd.getNumPartitions()
        new_partitions = initial_partitions - 1 if initial_partitions > 1 else 1
        self.test_dataset.coalesce(new_partitions)
        assert self.test_dataset._df.rdd.getNumPartitions() == new_partitions

    def test_initialize_without_schema(self: TestDataset, spark: SparkSession) -> None:
        """Test if Dataset derived class collects the schema from assets if schema is not provided."""
        df = spark.createDataFrame([(1,)], schema=MockDataset.get_schema())
        ds = MockDataset(_df=df)
        assert ds.schema == MockDataset.get_schema(), (
            "Schema should be inferred from df"
        )

    def test_passing_incorrect_types(self: TestDataset, spark: SparkSession) -> None:
        """Test if passing incorrect object types to Dataset raises an error."""
        with pytest.raises(TypeError):
            MockDataset(_df="not a dataframe")
        with pytest.raises(TypeError):
            MockDataset(_df=self.df, _schema="not a schema")

    @pytest.fixture(autouse=True)
    def _setup(self: TestDataset, spark: SparkSession) -> None:
        """Setup fixture."""
        df = spark.createDataFrame([(1,), (2,), (3,)], schema=MockDataset.get_schema())
        self.df = df
        self.test_dataset = MockDataset(_df=df, _schema=MockDataset.get_schema())


def test_dataset_filter(mock_study_index: StudyIndex) -> None:
    """Test Dataset.filter."""
    expected_filter_value = "gwas"
    condition = f.col("studyType") == expected_filter_value

    filtered = mock_study_index.filter(condition)
    assert (
        filtered.df.select("studyType").distinct().toPandas()["studyType"].to_list()[0]
        == expected_filter_value
    ), "Filtering failed."


def test_dataset_drop_infinity_values(spark: SparkSession) -> None:
    """drop_infinity_values method shoud remove inf value from standardError field."""
    data = [np.inf, -np.inf, 5.1]
    rows = [(v,) for v in data]
    schema = StructType([StructField("field", DoubleType())])
    input_df = spark.createDataFrame(rows, schema=schema)

    assert input_df.count() == 3
    # run without specifying *cols results in no filtering
    ds = MockDataset(_df=input_df, _schema=schema)
    assert ds.drop_infinity_values().df.count() == 3
    # otherwise drop all columns
    assert ds.drop_infinity_values("field").df.count() == 1


def test_process_class_params(spark: SparkSession) -> None:
    """Test splitting of parameters between class and spark parameters."""
    params = {
        "_df": spark.createDataFrame([(1,)], schema=MockDataset.get_schema()),
        "recursiveFileLookup": True,
    }
    class_params, spark_params = Dataset._process_class_params(params)
    assert "_df" in class_params, "Class params should contain _df"
    assert "recursiveFileLookup" in spark_params, (
        "Spark params should contain recursiveFileLookup"
    )


def test_flag_duplicates_retains_one_row_per_key(spark: SparkSession) -> None:
    """Without a caller-supplied ordering, exactly one row per key is left unflagged.

    Which row that is depends on the physical layout of the data and is not guaranteed across
    runs, so only the count is asserted here. Callers that need a defined survivor pass
    `order_by`, covered by `test_flag_duplicates_honours_order_by`.
    """
    df = spark.createDataFrame(
        [("id1", "a"), ("id1", "b"), ("id1", "c"), ("id2", "d")],
        ["key", "payload"],
    )

    retained = (
        df.withColumn("isDuplicate", Dataset.flag_duplicates(f.col("key")))
        .filter(~f.col("isDuplicate"))
        .collect()
    )

    assert {row["key"] for row in retained} == {"id1", "id2"}
    assert len(retained) == 2, "one row per key should be retained"


def test_flag_duplicates_honours_order_by(spark: SparkSession) -> None:
    """The caller-supplied ordering decides which duplicate is retained."""
    df = spark.createDataFrame(
        [("id1", 1), ("id1", 3), ("id1", 2)],
        ["key", "rank"],
    )

    def retained(order_by: list[Column]) -> int:
        """The `rank` of the row that is not flagged as a duplicate."""
        rows = (
            df.withColumn(
                "isDuplicate", Dataset.flag_duplicates(f.col("key"), order_by=order_by)
            )
            .filter(~f.col("isDuplicate"))
            .collect()
        )
        assert len(rows) == 1
        return rows[0]["rank"]

    assert retained([f.col("rank").asc()]) == 1
    assert retained([f.col("rank").desc()]) == 3
