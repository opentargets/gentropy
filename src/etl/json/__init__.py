from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")


def validate_df_schema(df: DataFrame, schema_json: str):
    core_schema = json.loads(Path(SCHEMA_DIR, schema_json).read_text(encoding="utf-8"))
    expected_schema = StructType.fromJson(core_schema)
    observed_schema = df.schema
    missing_struct_fields = [x for x in observed_schema if x not in expected_schema]
    error_message = "The {missing_struct_fields} StructFields are not included in the {schema_json} DataFrame schema: {all_struct_fields}".format(
        missing_struct_fields=missing_struct_fields,
        schema_json=schema_json,
        all_struct_fields=expected_schema,
    )
    if missing_struct_fields:
        raise Exception(error_message)
