"""Methods for handling schemas."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from collections import defaultdict, namedtuple
from typing import Any

import pyspark.sql.types as t
from pyspark.sql.types import ArrayType, StructType

from gentropy.assets import schemas


class SchemaValidationError(Exception):
    """This exception is raised when a schema validation fails."""

    def __init__(
        self: SchemaValidationError, message: str, errors: defaultdict[str, list[str]]
    ):
        """Initialize the SchemaValidationError.

        Args:
            message (str): The message to be displayed.
            errors (defaultdict[str, list[str]]): The collection of observed discrepancies
        """
        super().__init__(message)
        self.message = message  # Explicitly set the message attribute
        self.errors = errors

    def __str__(self: SchemaValidationError) -> str:
        """Return a string representation of the exception."""
        stringified_errors = "\n  ".join(
            [f'{k}: {",".join(v)}' for k, v in self.errors.items()]
        )
        return f"{self.message}\nErrors:\n  {stringified_errors}"


def parse_spark_schema(schema_json: str) -> StructType:
    """Parse Spark schema from JSON.

    Args:
        schema_json (str): JSON filename containing spark schema in the schemas package

    Returns:
        t.StructType: Spark schema
    """
    core_schema = json.loads(
        pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
    )
    return StructType.fromJson(core_schema)


def flatten_schema(schema: StructType, prefix: str = "") -> list[Any]:
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
        if isinstance(dtype, StructType):
            fields.append(Field(name, ArrayType(StructType())))
            fields += flatten_schema(dtype, prefix=name)
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            fields.append(Field(name, ArrayType(StructType())))
            fields += flatten_schema(dtype.elementType, prefix=name)
        else:
            fields.append(Field(name, dtype))
    return fields


def compare_array_schemas(
    observed_schema: ArrayType,
    expected_schema: ArrayType,
    parent_field_name: str | None = None,
    schema_issues: defaultdict[str, list[str]] | None = None,
) -> defaultdict[str, list[str]]:
    """Compare two array schemas.

    The comparison is done recursively, so nested structs are also compared.

    Args:
        observed_schema (ArrayType): The observed schema.
        expected_schema (ArrayType): The expected schema.
        parent_field_name (str, optional): The parent field name. Defaults to None.
        schema_issues (defaultdict[str, list[str]], optional): The schema issues. Defaults to None.

    Returns:
        defaultdict[str, list[str]]: The schema issues.
    """
    # Create default values if not provided:
    if schema_issues is None:
        schema_issues = defaultdict(list)

    if parent_field_name is None:
        parent_field_name = ""

    observed_type = observed_schema.elementType.typeName()
    expected_type = expected_schema.elementType.typeName()

    # If element types are not matching, no further tests are needed:
    if observed_type != expected_type:
        schema_issues["columns_with_non_matching_type"].append(
            f'For column "{parent_field_name}[]" found {observed_type} instead of {expected_type}'
        )

    # If element type is a struct, resolve nesting:
    elif observed_type == "struct":
        schema_issues = compare_struct_schemas(
            observed_schema.elementType,
            expected_schema.elementType,
            f"{parent_field_name}[].",
            schema_issues,
        )

    # If element type is an array, resolve nesting:
    elif observed_type == "array":
        schema_issues = compare_array_schemas(
            observed_schema.elementType,
            expected_schema.elementType,
            parent_field_name,
            schema_issues,
        )

    return schema_issues


def compare_struct_schemas(
    observed_schema: StructType,
    expected_schema: StructType,
    parent_field_name: str | None = None,
    schema_issues: defaultdict[str, list[str]] | None = None,
) -> defaultdict[str, list[str]]:
    """Compare two struct schemas.

    The comparison is done recursively, so nested structs are also compared.

    Args:
        observed_schema (StructType): The observed schema.
        expected_schema (StructType): The expected schema.
        parent_field_name (str, optional): The parent field name. Defaults to None.
        schema_issues (defaultdict[str, list[str]], optional): The schema issues. Defaults to None.

    Returns:
        defaultdict[str, list[str]]: The schema issues.
    """
    # Create default values if not provided:
    if schema_issues is None:
        schema_issues = defaultdict(list)

    if parent_field_name is None:
        parent_field_name = ""

    # Flagging duplicated columns if present:
    if duplicated_columns := list(
        {
            f"{parent_field_name}{field.name}"
            for field in observed_schema
            if list(observed_schema).count(field) > 1
        }
    ):
        schema_issues["duplicated_columns"] = duplicated_columns

    # Testing mandatory fields:
    required_fields = [x.name for x in expected_schema if not x.nullable]
    if missing_required_fields := [
        f"{parent_field_name}{req}"
        for req in required_fields
        if not any(field.name == req for field in observed_schema)
    ]:
        schema_issues["missing_mandatory_columns"] = missing_required_fields

    # Converting schema to dictionaries for easier comparison:
    observed_schema_dict = {field.name: field for field in observed_schema}
    expected_schema_dict = {field.name: field for field in expected_schema}

    # Testing optional fields and types:
    for field_name, field in observed_schema_dict.items():
        # Testing observed field name, if name is not matched, no further tests are needed:
        if field_name not in expected_schema_dict:
            schema_issues["unexpected_columns"].append(
                f"{parent_field_name}{field_name}"
            )
            continue

        # When we made sure the field is in both schemas, extracting field type information:
        observed_type = field.dataType
        observed_type_name = field.dataType.typeName()

        expected_type = expected_schema_dict[field_name].dataType
        expected_type_name = expected_schema_dict[field_name].dataType.typeName()

        # Flagging non-matching types if types don't match, jumping to next field:
        if observed_type_name != expected_type_name:
            schema_issues["columns_with_non_matching_type"].append(
                f'For column "{parent_field_name}{field_name}" found {observed_type_name} instead of {expected_type_name}'
            )
            continue

        # If column is a struct, resolve nesting:
        if observed_type_name == "struct":
            schema_issues = compare_struct_schemas(
                observed_type,
                expected_type,
                f"{parent_field_name}{field_name}.",
                schema_issues,
            )
        # If column is an array, resolve nesting:
        elif observed_type_name == "array":
            schema_issues = compare_array_schemas(
                observed_type,
                expected_type,
                f"{parent_field_name}{field_name}[]",
                schema_issues,
            )

    return schema_issues
