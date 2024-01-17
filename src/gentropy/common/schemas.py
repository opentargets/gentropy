"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from collections import namedtuple
from typing import Any

import pyspark.sql.types as t

from gentropy.assets import schemas


def parse_spark_schema(schema_json: str) -> t.StructType:
    """Parse Spark schema from JSON.

    Args:
        schema_json (str): JSON filename containing spark schema in the schemas package

    Returns:
        t.StructType: Spark schema
    """
    core_schema = json.loads(
        pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
    )
    return t.StructType.fromJson(core_schema)


def flatten_schema(schema: t.StructType, prefix: str = "") -> list[Any]:
    """It takes a Spark schema and returns a list of all fields in the schema once flattened.

    Args:
        schema (t.StructType): The schema of the dataframe
        prefix (str): The prefix to prepend to the field names. Defaults to "".

    Returns:
        list[Any]: A list of all the columns in the dataframe.

    Examples:
        >>> from pyspark.sql.types import ArrayType, StringType, StructField, StructType
        >>> schema = StructType(
        ...     [
        ...        StructField("studyLocusId", StringType(), False),
        ...        StructField("locus", ArrayType(StructType([StructField("variantId", StringType(), False)])), False)
        ...    ]
        ... )
        >>> df = spark.createDataFrame([("A", [{"variantId": "varA"}]), ("B", [{"variantId": "varB"}])], schema)
        >>> flatten_schema(df.schema)
        [Field(name='studyLocusId', dataType=StringType()), Field(name='locus', dataType=ArrayType(StructType([]), True)), Field(name='locus.variantId', dataType=StringType())]
    """
    Field = namedtuple("Field", ["name", "dataType"])
    fields = []
    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, t.StructType):
            fields.append(Field(name, t.ArrayType(t.StructType())))
            fields += flatten_schema(dtype, prefix=name)
        elif isinstance(dtype, t.ArrayType) and isinstance(
            dtype.elementType, t.StructType
        ):
            fields.append(Field(name, t.ArrayType(t.StructType())))
            fields += flatten_schema(dtype.elementType, prefix=name)
        else:
            fields.append(Field(name, dtype))
    return fields
