"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json

from pyspark.sql.types import StructType

from otg.assets import schemas


def parse_spark_schema(schema_json: str) -> StructType:
    """Parse Spark schema from JSON.

    Args:
        schema_json (str): JSON filename containing spark schema in the schemas package

    Returns:
        StructType: Spark schema
    """
    core_schema = json.loads(
        pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
    )
    return StructType.fromJson(core_schema)
