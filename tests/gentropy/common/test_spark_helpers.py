"""Tests spark-helper functions."""

from __future__ import annotations

import pytest
from gentropy.common.spark_helpers import (
    enforce_schema,
    order_array_of_structs_by_field,
)
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t


def test__order_array_of_structs_by_field(spark: SparkSession) -> None:
    """Test order_array_of_structs_by_field."""
    data = [
        # Values are the same:
        ("a", 12),
        ("a", 12),
        # First value bigger:
        ("b", 12),
        ("b", 1),
        # Second value bigger:
        ("c", 1),
        ("c", 12),
        # First value is null:
        ("d", None),
        ("d", 12),
        # Second value is null:
        ("e", 12),
        ("e", None),
        # Both values are null:
        ("f", None),
        ("f", None),
    ]

    processed_data = (
        spark.createDataFrame(data, ["group", "value"])
        .groupBy("group")
        .agg(
            f.collect_list(f.struct(f.col("value").alias("value"))).alias("values"),
            f.max(f.col("value")).alias("max_value"),
        )
        .withColumn("sorted_values", order_array_of_structs_by_field("values", "value"))
        .withColumn("sorted_max", f.col("sorted_values")[0].getField("value"))
        .select("max_value", "sorted_max")
        .collect()
    )

    for row in processed_data:
        assert row["max_value"] == row["sorted_max"]


class TestEnforceSchema:
    """Test enforce schema."""

    EXPECTED_SCHEMA = t.StructType(
        [
            t.StructField("field1", t.StringType(), True),
            t.StructField("field2", t.StringType(), True),
            t.StructField("field3", t.StringType(), True),
        ]
    )

    @staticmethod
    @enforce_schema(expected_schema=EXPECTED_SCHEMA)
    def test_function_good_schema() -> Column:
        """Create a struct with the expected schema."""
        return f.struct(
            f.lit("test1").alias("field1"),
            f.lit("test2").alias("field2"),
            f.lit("test3").alias("field3"),
        )

    @staticmethod
    @enforce_schema(expected_schema=EXPECTED_SCHEMA)
    def test_function_missing_column() -> Column:
        """Create a struct with a missing column."""
        return f.struct(
            f.lit("test1").alias("field1"),
            f.lit("test3").alias("field3"),
        )

    @staticmethod
    @enforce_schema(expected_schema=EXPECTED_SCHEMA)
    def test_function_wrong_order() -> Column:
        """Create a struct with the wrong order."""
        return f.struct(
            f.lit("test2").alias("field2"),
            f.lit("test1").alias("field1"),
        )

    @staticmethod
    @enforce_schema(expected_schema=EXPECTED_SCHEMA)
    def test_function_extra_column() -> Column:
        """Create a struct with an extra column."""
        return f.struct(
            f.lit("test2").alias("field2"),
            f.lit("test1").alias("field1"),
            f.lit("test4").alias("field4"),
        )

    @staticmethod
    @enforce_schema(expected_schema=EXPECTED_SCHEMA)
    def test_function_wrong_type() -> Column:
        """Create a struct with the wrong type."""
        return f.struct(
            f.lit("test2").alias("field2"),
            f.lit("test1").alias("field1"),
            f.lit(5).cast(t.IntegerType()).alias("field3"),
        )

    @pytest.fixture(autouse=True)
    def _setup(self: TestEnforceSchema, spark: SparkSession) -> None:
        """Setup fixture."""
        self.test_dataset = (
            spark.createDataFrame(
                [("a",)],
                ["label"],
            )
            .withColumn("struct_1", self.test_function_good_schema())
            .withColumn("struct_2", self.test_function_missing_column())
            .withColumn("struct_3", self.test_function_wrong_order())
            .withColumn("struct_4", self.test_function_extra_column())
            .withColumn("struct_5", self.test_function_wrong_type())
        )

    def test_schema_consistency(self: TestEnforceSchema) -> None:
        """Test enforce schema consistency."""
        # Looping through all the struct column and test if the schema is consistent
        for column in ["struct_1", "struct_2", "struct_3", "struct_4", "struct_5"]:
            assert self.test_dataset.schema[column].dataType == self.EXPECTED_SCHEMA
        for column in ["struct_1", "struct_2", "struct_3", "struct_4", "struct_5"]:
            assert self.test_dataset.schema[column].dataType == self.EXPECTED_SCHEMA
