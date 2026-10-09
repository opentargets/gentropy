"""Tests methods dealing with schema comparison."""

from __future__ import annotations

from collections import defaultdict

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.schemas import (
    compare_array_schemas,
    compare_struct_schemas,
)


class TestSchemaComparisonMethods:
    """Class for testing schema comparison methods."""

    STRUCT_FIELD_STRING = StructField("a", StringType(), True)
    STRUCT_FIELD_STRING_MANDATORY = StructField("a", StringType(), False)
    STRUCT_FIELD_INTEGER = StructField("b", IntegerType(), True)
    STRUCT_FIELD_WRONGTYPE = StructField("a", IntegerType(), True)

    def test_struct_validation_return_type(self: TestSchemaComparisonMethods) -> None:
        """Test successful validation of StructType."""
        observed = StructType([self.STRUCT_FIELD_STRING, self.STRUCT_FIELD_INTEGER])
        expected = StructType([self.STRUCT_FIELD_STRING, self.STRUCT_FIELD_INTEGER])

        discrepancy = compare_struct_schemas(observed, expected)
        assert isinstance(discrepancy, defaultdict)

    def test_struct_validation_success(self: TestSchemaComparisonMethods) -> None:
        """Test successful validation of StructType."""
        observed = StructType([self.STRUCT_FIELD_STRING, self.STRUCT_FIELD_INTEGER])
        expected = StructType([self.STRUCT_FIELD_STRING, self.STRUCT_FIELD_INTEGER])

        discrepancy = compare_struct_schemas(observed, expected)
        assert not discrepancy

    def test_struct_validation_non_matching_type(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of StructType."""
        observed = StructType([self.STRUCT_FIELD_STRING])
        expected = StructType([self.STRUCT_FIELD_WRONGTYPE])

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "columns_with_non_matching_type" in discrepancy

    def test_struct_validation_missing_mandatory(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of StructType."""
        observed = StructType([self.STRUCT_FIELD_INTEGER])
        expected = StructType(
            [self.STRUCT_FIELD_STRING_MANDATORY, self.STRUCT_FIELD_INTEGER]
        )

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "missing_mandatory_columns" in discrepancy

        # Test that the right column is flagged as missing:
        assert (
            self.STRUCT_FIELD_STRING_MANDATORY.name
            in discrepancy["missing_mandatory_columns"]
        )

    def test_struct_validation_unexpected_column(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of StructType."""
        observed = StructType(
            [self.STRUCT_FIELD_STRING_MANDATORY, self.STRUCT_FIELD_INTEGER]
        )
        expected = StructType([self.STRUCT_FIELD_STRING_MANDATORY])

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "unexpected_columns" in discrepancy

        # Test that the right column is flagged as unexpected:
        assert self.STRUCT_FIELD_INTEGER.name in discrepancy["unexpected_columns"]

    def test_struct_validation_duplicated_columns(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of StructType."""
        observed = StructType(
            [
                self.STRUCT_FIELD_STRING,
                self.STRUCT_FIELD_STRING,
                self.STRUCT_FIELD_INTEGER,
            ]
        )
        expected = StructType([self.STRUCT_FIELD_STRING, self.STRUCT_FIELD_INTEGER])

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "duplicated_columns" in discrepancy

        # Test that the right column is flagged as duplicated:
        assert self.STRUCT_FIELD_STRING.name in discrepancy["duplicated_columns"]

    def test_struct_validation_success_nested_struct(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test successful validation of nested StructType."""
        nested_struct = StructType(
            [self.STRUCT_FIELD_STRING, self.STRUCT_FIELD_INTEGER]
        )

        observed = StructType([StructField("c", nested_struct)])
        expected = StructType([StructField("c", nested_struct)])

        discrepancy = compare_struct_schemas(observed, expected)
        assert not discrepancy

    def test_struct_validation_non_matching_type_nested_struct(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of nested StructType."""
        nested_struct = StructType([self.STRUCT_FIELD_STRING])

        observed = StructType([StructField("c", nested_struct)])
        expected = StructType(
            [StructField("c", StructType([self.STRUCT_FIELD_WRONGTYPE]))]
        )

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "columns_with_non_matching_type" in discrepancy

    def test_array_validation_success(self: TestSchemaComparisonMethods) -> None:
        """Test successful validation of ArrayType."""
        observed = ArrayType(StringType())
        expected = ArrayType(StringType())

        discrepancy = compare_array_schemas(observed, expected)
        assert not discrepancy

    def test_array_validation_non_matching_type(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of ArrayType."""
        observed = ArrayType(StringType())
        expected = ArrayType(IntegerType())

        discrepancy = compare_array_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "columns_with_non_matching_type" in discrepancy

    def test_array_validation_nested_array(self: TestSchemaComparisonMethods) -> None:
        """Test successful validation of nested ArrayType."""
        nested_array = ArrayType(StringType())

        observed = ArrayType(nested_array)
        expected = ArrayType(nested_array)

        discrepancy = compare_array_schemas(observed, expected)
        assert not discrepancy

    def test_array_validation_non_matching_type_nested_array(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of nested ArrayType."""
        observed = ArrayType(ArrayType(StringType()))
        expected = ArrayType(ArrayType(IntegerType()))

        discrepancy = compare_array_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "columns_with_non_matching_type" in discrepancy

    def test_struct_validation_success_nested_with_array(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test successful validation of nested StructType with ArrayType."""
        nested_array = StructField("a", ArrayType(StringType()), True)
        nested_struct = StructType([self.STRUCT_FIELD_STRING, nested_array])

        observed = StructType([StructField("c", nested_struct, True)])
        expected = StructType([StructField("c", nested_struct, True)])

        discrepancy = compare_struct_schemas(observed, expected)
        assert not discrepancy

    def test_struct_validation_non_matching_type_nested_with_array(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of nested StructType with ArrayType."""
        nested_array = StructField("a", ArrayType(StringType()), True)
        nested_array_wrong_type = StructField("a", ArrayType(IntegerType()), True)
        nested_struct = StructType([self.STRUCT_FIELD_STRING, nested_array])
        nested_struct_wrong_type = StructType(
            [self.STRUCT_FIELD_STRING, nested_array_wrong_type]
        )
        observed = StructType([StructField("c", nested_struct, True)])
        expected = StructType([StructField("c", nested_struct_wrong_type, True)])

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test that the discrepancy is in the field name:
        assert "columns_with_non_matching_type" in discrepancy

    def test_struct_validation_failing_with_multiple_reasons(
        self: TestSchemaComparisonMethods,
    ) -> None:
        """Test unsuccessful validation of StructType with multiple issues."""
        observed = StructType(
            [
                StructField(
                    "a",
                    ArrayType(
                        ArrayType(
                            StructType(
                                [
                                    StructField("a", IntegerType(), False),
                                    StructField("c", StringType(), True),
                                    StructField("c", StringType(), True),
                                ]
                            ),
                            False,
                        ),
                        False,
                    ),
                    False,
                ),
            ]
        )

        expected = StructType(
            [
                StructField(
                    "a",
                    ArrayType(
                        ArrayType(
                            StructType(
                                [
                                    StructField("b", IntegerType(), False),
                                    StructField("c", StringType(), True),
                                    StructField("d", StringType(), True),
                                ]
                            ),
                            False,
                        ),
                        False,
                    ),
                    False,
                ),
            ]
        )

        discrepancy = compare_struct_schemas(observed, expected)

        # Test there's a discrepancy:
        assert discrepancy

        # Test if the returned list of discrepancies is correct:
        assert discrepancy == defaultdict(
            list,
            {
                "duplicated_columns": ["a[][].c"],
                "missing_mandatory_columns": ["a[][].b"],
                "unexpected_columns": ["a[][].a"],
            },
        )
