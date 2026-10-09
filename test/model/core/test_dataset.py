"""Tests for the Dataset base class."""

from __future__ import annotations

import types
from typing import Any, cast

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset, DatasetError
from gentropy.model.core.schema import implements, optional
from gentropy.model.core.trait import Field, Trait
from gentropy.model.core.validation import SchemaValidationError


class HasStudyId(Trait):
    """One-column trait shared by the tests."""

    study_id = Field(t.StringType())


class HasRegion(Trait):
    """Three-column trait shared by the tests."""

    chromosome = Field(t.StringType())
    locus_start = Field(t.IntegerType())
    locus_end = Field(t.IntegerType())


class CanJoinToStudies(Trait):
    """Trait sharing the study identifier column with ``HasStudyId``."""

    study_id = Field(t.StringType())


class Loci(Dataset[HasStudyId | HasRegion]):
    """Named dataset with two traits and one optional column."""

    study_id = implements(HasStudyId.study_id)
    chromosome = implements(HasRegion.chromosome)
    locus_start = implements(HasRegion.locus_start)
    locus_end = implements(HasRegion.locus_end)
    confidence = optional(t.ByteType())


def _declare(base: Any, **columns: object) -> type[Dataset[Any]]:
    """Define a dataset class from keyword columns, as a class body would.

    Args:
        base (Any): The ``Dataset[...]`` base.
        **columns (object): Class attributes, in declaration order.

    Returns:
        type[Dataset[Any]]: The new class.
    """
    return cast(
        "type[Dataset[Any]]",
        types.new_class(
            "Declared", (base,), exec_body=lambda namespace: namespace.update(columns)
        ),
    )


def test_dataset_records_its_traits_and_builds_its_schema_in_order() -> None:
    """Attributes become columns in declaration order; traits come from the base."""
    assert Loci.traits == frozenset({HasStudyId, HasRegion})
    assert Loci.schema.fieldNames() == [
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
        "confidence",
    ]


def test_dataset_without_traits_is_rejected() -> None:
    """A dataset with no traits could not be passed to any method."""
    with pytest.raises(DatasetError, match="Bare.*Dataset\\[A \\| B\\]"):

        class Bare(Dataset):  # type: ignore[type-arg]
            note = optional(t.StringType())


def test_dataset_without_columns_is_rejected() -> None:
    """Every named dataset states its column layout."""
    with pytest.raises(DatasetError, match="NoLayout.*at least one column"):

        class NoLayout(Dataset[HasStudyId]):
            pass


@pytest.mark.parametrize(
    ("columns", "problem"),
    [
        (
            {"study_id": implements(HasStudyId.study_id)},
            "HasRegion.chromosome is not implemented",
        ),
        (
            {
                "study_id": optional(t.StringType()),
                "chromosome": implements(HasRegion.chromosome),
                "locus_start": implements(HasRegion.locus_start),
                "locus_end": implements(HasRegion.locus_end),
            },
            "HasStudyId.study_id is not implemented; column studyId exists "
            "but is not linked",
        ),
    ],
    ids=["no column", "column of the same name without a link"],
)
def test_every_trait_field_needs_an_explicit_link(
    columns: dict[str, object], problem: str
) -> None:
    """A column only counts for a trait when the dataset says so."""
    with pytest.raises(DatasetError, match=problem):
        _declare(Dataset[HasStudyId | HasRegion], **columns)


def test_linking_a_field_of_an_undeclared_trait_is_rejected() -> None:
    """The type checker only knows the traits in ``Dataset[...]``."""
    with pytest.raises(DatasetError, match="links HasRegion.chromosome.*HasRegion"):
        _declare(
            Dataset[HasStudyId],
            study_id=implements(HasStudyId.study_id),
            chromosome=implements(HasRegion.chromosome),
        )


def test_one_column_can_serve_two_traits() -> None:
    """Sharing a column is declared once, with every field it implements."""
    shared = _declare(
        Dataset[HasStudyId | CanJoinToStudies],
        study_id=implements(HasStudyId.study_id, CanJoinToStudies.study_id),
    )
    assert shared.schema.fieldNames() == ["studyId"]


def test_assigning_a_trait_field_directly_is_rejected_without_damaging_it() -> None:
    """``id = HasStudyId.study_id`` would rebind the trait's own field."""
    with pytest.raises(DatasetError, match="study_key.*implements"):

        class Direct(Dataset[HasStudyId]):
            study_key = HasStudyId.study_id

    assert (HasStudyId.study_id.attribute, HasStudyId.study_id.owner) == (
        "study_id",
        HasStudyId,
    )


@pytest.mark.parametrize(
    ("attribute", "problem"),
    [("schema", "schema.*reserved"), ("studyId", "studyId.*snake_case")],
    ids=["clashes with Dataset", "not snake_case"],
)
def test_column_attribute_names_are_checked(attribute: str, problem: str) -> None:
    """Column attributes must convert to a column name and not shadow the base."""
    with pytest.raises(DatasetError, match=problem):
        _declare(
            Dataset[HasStudyId],
            study_id=implements(HasStudyId.study_id),
            **{attribute: optional(t.StringType())},
        )


def test_dataset_attribute_reads_the_dataframe_column(spark: SparkSession) -> None:
    """``Loci(df).locus_start`` is the DataFrame's ``locusStart`` column."""
    loci = Loci(
        spark.createDataFrame(
            [("GCST1", "1", 100, 200)],
            "studyId string, chromosome string, locusStart int, locusEnd int",
        )
    )
    assert loci.df.select(loci.locus_start).first() == (100,)


def test_dataset_keeps_a_matching_dataframe_unchanged(spark: SparkSession) -> None:
    """Construction only validates: no reordering, and nullable flags are ignored."""
    df = spark.createDataFrame(
        [(3, 200, 100, "1", "GCST1")],
        "confidence byte, locusEnd int, locusStart int, chromosome string, studyId string",
    )
    assert Loci(df).df is df


def test_dataset_accepts_a_dataframe_without_optional_columns(
    spark: SparkSession,
) -> None:
    """An absent optional column means the same as a column of nulls."""
    df = spark.createDataFrame(
        [("GCST1", "1", 100, 200)],
        "studyId string, chromosome string, locusStart int, locusEnd int",
    )
    assert Loci(df).df is df


@pytest.mark.parametrize(
    ("ddl", "problem"),
    [
        (
            "studyId string, chromosome string, locusStart int",
            "missing_mandatory_columns: locusEnd",
        ),
        (
            "studyId string, chromosome string, locusStart int, locusEnd int, "
            "pValue double",
            "unexpected_columns: pValue",
        ),
        (
            "studyId string, chromosome string, locusStart int, locusEnd string",
            'columns_with_non_matching_type: For column "locusEnd"',
        ),
    ],
    ids=["trait column missing", "extra column", "wrong type"],
)
def test_dataset_rejects_a_dataframe_that_does_not_match_its_schema(
    spark: SparkSession, ddl: str, problem: str
) -> None:
    """Every mismatch the legacy validation reports stops construction."""
    df = spark.createDataFrame([], ddl)
    with pytest.raises(SchemaValidationError, match=problem):
        Loci(df)


def test_base_dataset_cannot_hold_data(spark: SparkSession) -> None:
    """Only named datasets have a schema to check the data against."""
    df = spark.createDataFrame([("GCST1",)], "studyId string")
    with pytest.raises(DatasetError, match="named dataset"):
        Dataset(df)


def test_print_schema_shows_mandatory_columns_and_their_trait_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The printed layout says which columns are mandatory and which fields they serve."""
    shared = _declare(
        Dataset[HasStudyId | CanJoinToStudies],
        id=implements(HasStudyId.study_id, CanJoinToStudies.study_id),
        confidence=optional(t.ByteType()),
    )
    shared.print_schema()
    assert capsys.readouterr().out == (
        "Declared\n"
        " |-- id: string (mandatory: HasStudyId.study_id, CanJoinToStudies.study_id)\n"
        " |-- confidence: tinyint (optional)\n"
    )


def test_print_schema_expands_structs_and_keeps_simple_types_inline(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Nested structs are drawn as a tree; arrays of plain values stay on one line."""

    class Staging(Dataset[HasStudyId]):
        study_id = implements(HasStudyId.study_id)
        locus = optional(
            t.ArrayType(
                t.StructType(
                    [
                        t.StructField("variantId", t.StringType()),
                        t.StructField("beta", t.DoubleType()),
                    ]
                )
            )
        )
        quality_controls = optional(t.ArrayType(t.StringType()))

    Staging.print_schema()
    assert capsys.readouterr().out == (
        "Staging\n"
        " |-- studyId: string (mandatory: HasStudyId.study_id)\n"
        " |-- locus: array (optional)\n"
        " |    |-- element: struct\n"
        " |    |    |-- variantId: string\n"
        " |    |    |-- beta: double\n"
        " |-- qualityControls: array<string> (optional)\n"
    )
