"""Tests for declaring dataset columns."""

from __future__ import annotations

import pytest
from pyspark.sql import types as t

from gentropy.model.core.schema import SchemaError, implements, optional, required
from gentropy.model.core.trait import Field, Trait


class HasVariantId(Trait):
    """Trait holding a variant identifier."""

    variant_id = Field(t.StringType())


class CanJoinToVariantIndex(Trait):
    """Trait for joining to the variant index on the same identifier."""

    variant_id = Field(t.StringType())


class HasPosition(Trait):
    """Trait with an integer column, to clash with string fields."""

    position = Field(t.IntegerType())


class Holder:
    """Plain class that names its columns, as a dataset does."""

    trait_free = optional(t.ByteType())
    trait_free_required = required(t.ByteType())
    lead_variant_id = implements(HasVariantId.variant_id)
    variant_id = implements(HasVariantId.variant_id, CanJoinToVariantIndex.variant_id)


def test_optional_column_is_nullable_without_metadata() -> None:
    """An optional column is named after its attribute and carries no trait link."""
    assert Holder.trait_free.struct_field == t.StructField(
        "traitFree", t.ByteType(), nullable=True
    )


def test_required_column_is_mandatory_without_a_trait_link() -> None:
    """A required column carries the marker, so construction checks it, but no link."""
    assert Holder.trait_free_required.struct_field == t.StructField(
        "traitFreeRequired", t.ByteType(), nullable=True, metadata={"mandatory": True}
    )


def test_implementing_column_is_named_by_the_dataset_and_linked_to_the_field() -> None:
    """The attribute names the column; the metadata names the trait field."""
    assert Holder.lead_variant_id.struct_field == t.StructField(
        "leadVariantId",
        t.StringType(),
        nullable=True,
        metadata={"mandatory": True, "traits": ["HasVariantId.variant_id"]},
    )


def test_one_column_can_implement_several_trait_fields() -> None:
    """Sharing a column between traits is declared, never inferred."""
    assert Holder.variant_id.struct_field.metadata == {
        "mandatory": True,
        "traits": ["HasVariantId.variant_id", "CanJoinToVariantIndex.variant_id"],
    }


def test_implements_needs_at_least_one_field() -> None:
    """A column that implements nothing is an optional column."""
    with pytest.raises(SchemaError, match="at least one trait field"):
        implements()


def test_implements_rejects_fields_of_different_types() -> None:
    """Two trait fields can share a column only if they hold the same type."""
    with pytest.raises(SchemaError, match="HasPosition.position is int"):
        implements(HasVariantId.variant_id, HasPosition.position)


def test_implements_rejects_a_field_outside_a_trait() -> None:
    """Only trait fields carry a link a method can rely on."""
    with pytest.raises(SchemaError, match="not declared on a trait"):
        implements(Field(t.StringType()))


def test_optional_rejects_a_type_that_is_not_a_spark_type() -> None:
    """A Python type in place of a Spark type fails at declaration."""
    with pytest.raises(SchemaError, match="Spark DataType"):
        optional(str)  # type: ignore[arg-type]


def test_required_rejects_a_type_that_is_not_a_spark_type() -> None:
    """A Python type in place of a Spark type fails at declaration."""
    with pytest.raises(SchemaError, match="Spark DataType"):
        required(str)  # type: ignore[arg-type]
