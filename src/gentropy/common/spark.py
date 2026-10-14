"""Helpers for Spark types and columns."""

from __future__ import annotations

from typing import cast

from pyspark.sql import types as t


def element_struct(dtype: t.DataType) -> t.StructType:
    """The struct held by each element of an array type.

    Code over nested columns reads the nested field names from the declared
    type with this, instead of naming them by hand.

    Args:
        dtype (t.DataType): An array of structs.

    Returns:
        t.StructType: The element struct.

    Examples:
        >>> dtype = t.ArrayType(t.StructType([t.StructField("tagVariantId", t.StringType())]))
        >>> element_struct(dtype).fieldNames()
        ['tagVariantId']
    """
    return cast(t.StructType, cast(t.ArrayType, dtype).elementType)
