"""Methods for handling schemas."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from collections import namedtuple
from typing import TYPE_CHECKING

import pyspark.sql.types as t

from otg.assets import schemas

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame


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


def flatten_schema(schema: t.StructType, prefix: str = "") -> list:
    """It takes a Spark schema and returns a list of all fields in the schema once flattened.

    Args:
        schema (t.StructType): The schema of the dataframe
        prefix (str): The prefix to prepend to the field names. Defaults to "".

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


def _get_spark_schema_from_pandas_df(pdf: PandasDataFrame) -> t.StructType:
    """Returns the Spark schema based on a Pandas DataFrame.

    Args:
        pdf (PandasDataFrame): Pandas DataFrame

    Returns:
        t.StructType: Spark schema

    Examples:
        >>> import pandas as pd
        >>> pdf = pd.DataFrame({"col1": [1, 2], "col2": [3.0, 4.0]})
        >>> _get_spark_schema_from_pandas_df(pdf)
        StructType([StructField('col1', IntegerType(), True), StructField('col2', FloatType(), True)])
    """

    def _get_spark_type(pandas_type: str) -> t.DataType:
        """Returns the Spark type based on the Pandas type.

        Args:
            pandas_type (str): Pandas type

        Returns:
            t.DataType: Spark type

        Raises:
            ValueError: If the Pandas type is not supported
        """
        try:
            if pandas_type == "object":
                return t.StringType()
            elif pandas_type == "int64":
                return t.IntegerType()
            elif pandas_type == "float64":
                return t.FloatType()
        except Exception as e:
            raise ValueError(f"Unsupported type: {pandas_type}") from e

    return t.StructType(
        [
            t.StructField(field, _get_spark_type(pdf[field].dtype), True)
            for field in pdf.columns
        ]
    )
