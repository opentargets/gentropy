"""Tests for traits and their fields."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements
from gentropy.model.core.trait import Field, Trait, TraitError


class HasA(Trait):
    """Trait with two columns, shared by the tests."""

    column_a = Field(t.StringType())
    column_b = Field(t.LongType())


def test_trait_lists_its_fields_in_declaration_order() -> None:
    """Schemas read a trait's columns from ``fields`` in declaration order."""
    assert [field.name for field in HasA.fields] == ["columnA", "columnB"]


@pytest.mark.parametrize(
    ("attribute", "column"),
    [
        ("study_id", "studyId"),
        ("p_value_mantissa", "pValueMantissa"),
        ("log10_bayes_factor", "log10BayesFactor"),
        ("r2_overall", "r2Overall"),
        ("beta", "beta"),
    ],
)
def test_field_column_name_is_the_camel_case_attribute(
    attribute: str, column: str
) -> None:
    """Snake-case attributes map to gentropy's camelCase column names."""
    trait = type("HasColumn", (Trait,), {attribute: Field(t.DoubleType())})
    assert getattr(trait, attribute).name == column


@pytest.mark.parametrize(
    "attribute", ["_hidden", "trailing_", "double__underscore", "studyId"]
)
def test_field_attribute_without_a_clean_camel_case_form_is_rejected(
    attribute: str,
) -> None:
    """Only snake_case attributes have a well-defined camelCase column name."""
    with pytest.raises(TraitError, match="snake_case"):
        type("HasColumn", (Trait,), {attribute: Field(t.DoubleType())})


def test_field_rejects_a_type_that_is_not_a_spark_type() -> None:
    """A Python type in place of a Spark type fails at declaration."""
    with pytest.raises(TraitError, match="Spark DataType"):
        Field(str)  # type: ignore[arg-type]


def test_trait_without_fields_is_rejected() -> None:
    """A trait must name at least one column."""
    with pytest.raises(TraitError, match="at least one Field"):

        class Empty(Trait):
            pass


def test_trait_inheriting_from_a_trait_is_rejected() -> None:
    """Traits combine through unions, never through inheritance."""
    with pytest.raises(TraitError, match="subclass Trait directly"):

        class HasMore(HasA):
            column_c = Field(t.DoubleType())


def test_trait_with_a_base_other_than_trait_is_rejected() -> None:
    """A field on another base would be silently left out of the trait's columns."""

    class Columns:
        column_x = Field(t.StringType())

    with pytest.raises(TraitError, match="subclass Trait directly"):

        class HasX(Trait, Columns):
            column_y = Field(t.StringType())


def test_trait_with_a_logic_only_base_is_rejected() -> None:
    """Traits carry columns only; logic lives in functions on columns."""

    class Logic:
        def describe(self) -> str:
            return "derived"

    with pytest.raises(TraitError, match="subclass Trait directly"):

        class HasX(Trait, Logic):
            column_x = Field(t.StringType())


class Pairs(Dataset[HasA]):
    """Dataset storing HasA's columns under their default names."""

    column_a = implements(HasA.column_a)
    column_b = implements(HasA.column_b)


def test_bound_trait_reads_the_dataset_column(spark: SparkSession) -> None:
    """``Trait.of(ds).field`` is the dataset's column for that field."""
    ds = Pairs(spark.createDataFrame([("x", 1)], "columnA string, columnB long"))
    assert ds.df.select(HasA.of(ds).column_a).first() == ("x",)


class Renamed(Dataset[HasA]):
    """Dataset storing HasA.column_a under its own column name."""

    id = implements(HasA.column_a)
    column_b = implements(HasA.column_b)


def test_bound_trait_follows_a_renamed_column(spark: SparkSession) -> None:
    """The dataset's own column name wins over the field's default name."""
    ds = Renamed(spark.createDataFrame([("x", 1)], "id string, columnB long"))
    assert ds.df.select(HasA.of(ds).column_a).first() == ("x",)


class HasZ(Trait):
    """Trait that ``Pairs`` does not declare."""

    column_z = Field(t.StringType())


def test_binding_a_trait_the_dataset_does_not_declare_is_rejected(
    spark: SparkSession,
) -> None:
    """The type checker rejects this; the runtime check covers untyped callers."""
    ds = Pairs(spark.createDataFrame([("x", 1)], "columnA string, columnB long"))
    with pytest.raises(TraitError, match="Pairs does not implement HasZ"):
        HasZ.of(ds)  # type: ignore[arg-type]


def test_constructing_a_trait_directly_points_to_of() -> None:
    """A trait is a declaration; its views come only from ``Trait.of(dataset)``."""
    with pytest.raises(TraitError, match=r"use HasA.of\(dataset\)"):
        HasA()
