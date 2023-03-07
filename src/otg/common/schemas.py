"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import pyspark.sql.types as t

from otg import schemas

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame


def parse_spark_schema(schema_json: str) -> t.StructType:
    """Parse Spark schema from JSON.

    Args:
        schema_json (str): JSON filename containing spark schema in the schemas package

    Returns:
        StructType: Spark schema
    """
    core_schema = json.loads(
        pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
    )
    return t.StructType.fromJson(core_schema)


def _get_spark_schema_from_pandas_df(pdf: PandasDataFrame) -> t.StructType:
    """Returns the Spark schema based on a Pandas DataFrame."""
    return t.StructType(
        [
            t.StructField(field, _get_spark_type(pdf[field].dtype), True)
            for field in pdf.columns
        ]
    )


def _get_spark_type(pandas_type: str) -> t.DataType:
    """Returns the Spark type based on the Pandas type."""
    try:
        if pandas_type == "object":
            return t.StringType()
        elif pandas_type == "int64":
            return t.IntegerType()
        elif pandas_type == "float64":
            return t.FloatType()
    except Exception as e:
        raise ValueError(f"Unsupported type: {pandas_type}") from e
