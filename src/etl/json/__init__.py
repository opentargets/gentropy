"""JSON helper functions."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

from pyspark.sql.types import StructType

from etl.json import schemas

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def validate_df_schema(df: DataFrame, schema_json: str) -> None:
    """Validate DataFrame schema based on JSON.

    Args:
        df (DataFrame): DataFrame to validate
        schema_json (str): schema name (e.g. targets.json)

    Raises:
        Exception: DataFrame schema is not valid
    """
    core_schema = json.loads(
        pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
    )
    expected_schema = StructType.fromJson(core_schema)
    observed_schema = df.schema
    # Observed fields not in schema
    missing_struct_fields = [x for x in observed_schema if x not in expected_schema]
    error_message = f"The {missing_struct_fields} StructFields are not included in the {schema_json} DataFrame schema: {expected_schema}"
    if missing_struct_fields:
        raise Exception(error_message)

    # Required fields not in dataset
    required_fields = [x for x in expected_schema if not x.nullable]
    missing_required_fields = [x for x in required_fields if x not in observed_schema]
    error_message = f"The {missing_required_fields} StructFields are required but missing from the DataFrame schema: {expected_schema}"
    if missing_required_fields:
        raise Exception(error_message)
