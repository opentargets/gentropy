"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json

from pyspark.sql.types import ArrayType, StructType

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


def flatten_schema(schema: StructType, prefix: str = "") -> list:
    """It takes a Spark schema and returns a list of all fields in the schema once flattened.

    Args:
        schema: The schema of the dataframe
        prefix: The prefix to prepend to the field names.

    Returns:
        list: A list of all the columns in the dataframe.

    Examples:
        >>> from pyspark.sql.types import ArrayType, StringType, StructField, StructType
        >>> schema = StructType(
        ...     [
        ...        StructField("studyLocusId", StringType(), False),
        ...        StructField("credibleSet", ArrayType(StructType([StructField("tagVariantId", StringType(), False)])), False)
        ...    ]
        ... )
        >>> df = spark.createDataFrame([("A", [{"tagVariantId": "varA"}]), ("B", [{"tagVariantId": "varB"}])], schema)
        >>> flatten_schema(df.schema)
        [('studyLocusId', StringType), ('credibleSet', ArrayType(StructType(List(StructField(tagVariantId,StringType,false))),true)), ('credibleSet.tagVariantId', StringType)]
    """
    fields = []
    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
        dtype = field.dataType
        fields.append((name, dtype))
        if isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name)
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            fields += flatten_schema(dtype.elementType, prefix=name)
    return fields
