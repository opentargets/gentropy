"""Test fine-mapping method constraints.

Happy-path behaviour for each MethodConstraint is covered by doctests in
gentropy.method.fine_mapping.constraint. This file only covers edge cases
that are awkward to express in a doctest (nulls, ordering nuances) plus the
shared annotate() output shape.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from gentropy.common.types import LDPopulation
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.method.fine_mapping.constraint import (
    ConstraintResult,
    HasAllowedMajorAncestry,
    HasMappedTrait,
    HasSumstats,
)

STUDY_REQUIRED_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, hasSumstats BOOLEAN, "
    "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>, "
    "traitFromSourceMappedIds ARRAY<STRING>"
)


def _study_index(spark: SparkSession, **overrides: object) -> StudyIndex:
    """Build a single-row StudyIndex with sensible defaults, overridable per test."""
    row = {
        "studyId": "s1",
        "projectId": "p",
        "studyType": StudyType.GWAS.value,
        "hasSumstats": False,
        "ldPopulationStructure": [(LDPopulation.NFE.value, 1.0)],
        "traitFromSourceMappedIds": ["EFO_1"],
        **overrides,
    }
    data = [
        (
            row["studyId"],
            row["projectId"],
            row["studyType"],
            row["hasSumstats"],
            row["ldPopulationStructure"],
            row["traitFromSourceMappedIds"],
        )
    ]
    return StudyIndex(_df=spark.createDataFrame(data, STUDY_REQUIRED_SCHEMA))


def _constraint_value(result: ConstraintResult, name: str) -> bool:
    constraints = result.df.collect()[0]["constraints"]
    return next(c["value"] for c in constraints if c["name"] == name)


def test_annotate_output_schema(spark: SparkSession) -> None:
    """annotate() produces the ConstraintResult schema: studyId + constraints[name, value]."""
    si = _study_index(spark)
    result = HasSumstats().annotate(si)
    assert isinstance(result, ConstraintResult)
    assert result.df.columns == ["studyId", "constraints"]
    row = result.df.collect()[0]
    assert row["studyId"] == "s1"
    assert row["constraints"] == [("hasSumstats", False)]


@pytest.mark.parametrize(
    ["trait_ids", "expected"],
    [
        pytest.param([], False, id="empty trait list is disallowed"),
        pytest.param(None, False, id="null trait list is disallowed"),
        pytest.param(["EFO_1", "EFO_2"], True, id="multiple mapped traits allowed"),
    ],
)
def test_has_mapped_trait_edge_cases(
    spark: SparkSession, trait_ids: list[str] | None, expected: bool
) -> None:
    """HasMappedTrait must treat both null and empty arrays as no mapped trait."""
    si = _study_index(spark, traitFromSourceMappedIds=trait_ids)
    result = HasMappedTrait().annotate(si)
    assert _constraint_value(result, "hasMappedTrait") is expected


def test_has_allowed_major_ancestry_picks_highest_relative_sample_size(
    spark: SparkSession,
) -> None:
    """The major ancestry is the one with the highest relativeSampleSize, regardless of array order."""
    si = _study_index(
        spark,
        ldPopulationStructure=[
            (LDPopulation.AFR.value, 0.3),
            (LDPopulation.NFE.value, 0.7),
        ],
    )
    constraint = HasAllowedMajorAncestry(
        allowed_ancestries=[LDPopulation.NFE],
        relative_sample_size_threshold=0.5,
    )
    result = constraint.annotate(si)
    assert _constraint_value(result, "hasAllowedMajorAncestry") is True


def test_has_allowed_major_ancestry_at_exact_threshold(spark: SparkSession) -> None:
    """A relative sample size exactly equal to the threshold is allowed (>=, not >)."""
    si = _study_index(spark, ldPopulationStructure=[(LDPopulation.NFE.value, 0.5)])
    constraint = HasAllowedMajorAncestry(
        allowed_ancestries=[LDPopulation.NFE],
        relative_sample_size_threshold=0.5,
    )
    result = constraint.annotate(si)
    assert _constraint_value(result, "hasAllowedMajorAncestry") is True
