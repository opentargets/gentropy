"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from collections import namedtuple

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
        if isinstance(dtype, StructType):
            fields.append(Field(name, ArrayType(StructType())))
            fields += flatten_schema(dtype, prefix=name)
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            fields.append(Field(name, ArrayType(StructType())))
            fields += flatten_schema(dtype.elementType, prefix=name)
        else:
            fields.append(Field(name, dtype))
    return fields


def filter_schema(schema: StructType, fields_of_interest: list) -> StructType:
    """Filter the fields in a Pyspark schema of type StructType given a list of fields of interest.

    !!! info "Fields in the returned schema will follow the same as the order they appear in the original schema."

    Args:
        schema: The schema of the dataframe
        fields_of_interest: The list of fields to keep in the schema

    Returns:
        StructType: The filtered schema

    Examples:
        >>> from pyspark.sql.types import StringType, StructField, StructType
        >>> original_schema = StructType([
        ...        StructField("A", StringType(), True),
        ...        StructField("B", StringType(), True)
        ... ])
        >>> filter_schema(original_schema, ["A"])
        StructType([StructField('A', StringType(), True)])
    """
    return StructType(
        [field for field in schema.fields if field.name in fields_of_interest]
    )
