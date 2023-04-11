"""Tests on spark schemas."""
# type: ignore
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql.types import StringType, StructField, StructType

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

    @pytest.fixture(scope="class")
    def mock_expected_schema(self: TestValidateSchema) -> StructType:
        """Mock expected schema."""
        return StructType(
            [
                StructField("studyLocusId", StringType(), nullable=False),
                StructField("geneId", StringType(), nullable=False),
            ]
        )

    def test_validate_schema_extra_field(
        self: TestValidateSchema, spark: SparkSession, mock_expected_schema: StructType
    ) -> None:
        """Test that validate_schema raises an error if the observed schema has an extra field."""
        df = spark.createDataFrame(
            [("A", "ENSG0001", "extra1"), ("B", "ENSG0002", "extra2")],
            schema=["studyLocusId", "geneId", "extraField"],
        )
        with pytest.raises(ValueError, match="extraField"):
            Dataset(df, mock_expected_schema)

    def test_validate_schema_missing_field(
        self: TestValidateSchema, spark: SparkSession, mock_expected_schema: StructType
    ) -> None:
        """Test that validate_schema raises an error if the observed schema is missing a required field."""
        df = spark.createDataFrame([("A",), ("B",)], schema=["geneId"])
        with pytest.raises(ValueError, match="geneId"):
            Dataset(df, mock_expected_schema)
