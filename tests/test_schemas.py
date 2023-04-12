"""Tests on spark schemas."""
# type: ignore
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SCHEMA_DIR = "src/otg/assets/schemas"


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Testing accross all schemas.

    Pytest hook to parametrise testing

    Args:
        metafunc (Metafunc): _description_
    """
    if "schema_json" in metafunc.fixturenames:
        schemas = [f for f in os.listdir(SCHEMA_DIR) if f.endswith(".json")]
        metafunc.parametrize("schema_json", schemas)


def test_schema(schema_json: str) -> None:
    """Test schema is a valid Spark schema.

    Args:
        schema_json (str): schema filename
    """
    core_schema = json.loads(Path(SCHEMA_DIR, schema_json).read_text(encoding="utf-8"))
    isinstance(StructType.fromJson(core_schema), StructType)


class TestValidateSchema:
    """Test validate_schema method."""

    mock_expected_schema = StructType(
        [
            StructField("studyLocusId", StringType(), nullable=False),
            StructField("geneId", StringType(), nullable=False),
        ]
    )
    mock_expected_nested_schema = StructType(
        [
            StructField("studyLocusId", StringType(), nullable=False),
            StructField(
                "credibleSet",
                ArrayType(
                    StructType([StructField("tagVariantId", StringType(), False)])
                ),
                False,
            ),
        ]
    )
    mock_observed_data = [("A", "ENSG0001"), ("B", "ENSG0002")]
    mock_observed_nested_data = [
        ("A", [{"tagVariantId": "varA"}]),
        ("B", [{"tagVariantId": "varB"}]),
    ]

    @pytest.mark.parametrize(
        ("observed_data", "expected_schema"),
        [
            (mock_observed_data, mock_expected_schema),
            (mock_observed_nested_data, mock_expected_nested_schema),
        ],
    )
    def test_validate_schema_extra_field(
        self: TestValidateSchema,
        spark: SparkSession,
        observed_data: list,
        expected_schema: StructType,
    ) -> None:
        """Test that validate_schema raises an error if the observed schema has an extra field."""
        df = spark.createDataFrame(
            observed_data,
            schema=expected_schema,
        ).withColumn("extraField", f.lit("extra"))
        with pytest.raises(ValueError, match="extraField"):
            Dataset(df, expected_schema)

    @pytest.mark.parametrize(
        ("observed_data", "expected_schema"),
        [
            (mock_observed_data, mock_expected_schema),
            (mock_observed_nested_data, mock_expected_nested_schema),
        ],
    )
    def test_validate_schema_missing_field(
        self: TestValidateSchema,
        spark: SparkSession,
        observed_data: list,
        expected_schema: StructType,
    ) -> None:
        """Test that validate_schema raises an error if the observed schema is missing a required field."""
        df = spark.createDataFrame(
            observed_data,
            schema=expected_schema,
        ).drop("studyLocusId")
        with pytest.raises(ValueError, match="studyLocusId"):
            Dataset(df, expected_schema)

    @pytest.mark.parametrize(
        ("observed_data", "expected_schema"),
        [
            (mock_observed_data, mock_expected_schema),
            (mock_observed_nested_data, mock_expected_nested_schema),
        ],
    )
    def test_validate_schema_different_datatype(
        self: TestValidateSchema,
        spark: SparkSession,
        observed_data: list,
        expected_schema: StructType,
    ) -> None:
        """Test that validate_schema raises an error if any field in the observed schema has a different type than expected."""
        df = spark.createDataFrame(
            observed_data,
            schema=expected_schema,
        ).withColumn("studyLocusId", f.lit(1))
        with pytest.raises(ValueError, match="studyLocusId"):
            Dataset(df, expected_schema)
