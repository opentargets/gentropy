"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json

import pyspark.sql.types as t

from otg import schemas


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
