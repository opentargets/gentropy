"""Test fine-mapping method constraints.

Happy-path behaviour for each MethodConstraint is covered by doctests in
gentropy.method.fine_mapping.constraint. This file only covers edge cases
that are awkward to express in a doctest (nulls, ordering nuances).
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from gentropy.common.types import LDPopulation
from gentropy.dataset.study_index import StudyIndex, StudyQualityCheck, StudyType
from gentropy.method.fine_mapping.constraint import (
    HasAllowedMajorAncestry,
    HasMappedTrait,
    HasSufficientESS,
    MethodConstraint,
    PassSumstatQC,
)

STUDY_REQUIRED_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, hasSumstats BOOLEAN, "
    "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>, "
    "traitFromSourceMappedIds ARRAY<STRING>, qualityControls ARRAY<STRING>"
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
        "qualityControls": [],
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
            row["qualityControls"],
        )
    ]
    return StudyIndex(_df=spark.createDataFrame(data, STUDY_REQUIRED_SCHEMA))


def _constraint_value(si: StudyIndex, constraint: MethodConstraint) -> bool:
    """Evaluate a single constraint's expression against a StudyIndex and return its value."""
    return si.df.select(constraint.expression.alias("value")).collect()[0]["value"]


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
    assert _constraint_value(si, HasMappedTrait()) is expected


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
    assert _constraint_value(si, constraint) is True


def test_has_allowed_major_ancestry_at_exact_threshold(spark: SparkSession) -> None:
    """A relative sample size exactly equal to the threshold is allowed (>=, not >)."""
    si = _study_index(spark, ldPopulationStructure=[(LDPopulation.NFE.value, 0.5)])
    constraint = HasAllowedMajorAncestry(
        allowed_ancestries=[LDPopulation.NFE],
        relative_sample_size_threshold=0.5,
    )
    assert _constraint_value(si, constraint) is True


@pytest.mark.parametrize(
    ["quality_controls", "expected"],
    [
        pytest.param([], True, id="empty quality control list passes"),
        pytest.param(None, False, id="null quality control list is disallowed"),
        pytest.param(
            [StudyQualityCheck.UNRESOLVED_TARGET.value],
            False,
            id="disallowed reason present fails",
        ),
    ],
)
def test_pass_sumstat_qc_edge_cases(
    spark: SparkSession, quality_controls: list[str] | None, expected: bool
) -> None:
    """PassSumstatQC must treat a null qualityControls array as failing, not unknown."""
    si = _study_index(spark, qualityControls=quality_controls)
    constraint = PassSumstatQC(disallowed_reasons=[StudyQualityCheck.UNRESOLVED_TARGET])
    assert _constraint_value(si, constraint) is expected


ESS_STUDY_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, "
    "qualityControls ARRAY<STRING>, nSamples INT, nCases INT, nControls INT"
)


def _ess_study_index(
    spark: SparkSession,
    quality_controls: list[str] | None,
    n_samples: int | None,
    n_cases: int | None,
    n_controls: int | None,
) -> StudyIndex:
    """Build a single-row StudyIndex with the sample-size columns used by HasSufficientESS."""
    data = [
        (
            "s1",
            "p",
            StudyType.GWAS.value,
            quality_controls,
            n_samples,
            n_cases,
            n_controls,
        )
    ]
    return StudyIndex(_df=spark.createDataFrame(data, ESS_STUDY_SCHEMA))


def test_has_sufficient_ess_at_exact_threshold_passes(spark: SparkSession) -> None:
    """A sample size exactly equal to the threshold is sufficient (>=, not >)."""
    si = _ess_study_index(
        spark, [StudyQualityCheck.MEASUREMENT_STUDY_DESIGN.value], 1000, None, None
    )
    constraint = HasSufficientESS(min_ess=1000)
    assert _constraint_value(si, constraint) is True


@pytest.mark.parametrize(
    ["quality_controls", "n_samples", "n_cases", "n_controls"],
    [
        pytest.param(
            [StudyQualityCheck.CASE_CONTROL_STUDY_DESIGN.value],
            200,
            None,
            None,
            id="case-control design with null counts",
        ),
        pytest.param(
            [StudyQualityCheck.MEASUREMENT_STUDY_DESIGN.value],
            None,
            None,
            None,
            id="measurement design with null nSamples",
        ),
        pytest.param(
            [],
            1_000_000,
            500_000,
            500_000,
            id="undetermined study design",
        ),
        pytest.param(None, 1_000_000, None, None, id="null quality controls"),
    ],
)
def test_has_sufficient_ess_fails_without_computable_sample_size(
    spark: SparkSession,
    quality_controls: list[str] | None,
    n_samples: int | None,
    n_cases: int | None,
    n_controls: int | None,
) -> None:
    """HasSufficientESS must treat an uncomputable effective sample size as failing, not unknown."""
    si = _ess_study_index(spark, quality_controls, n_samples, n_cases, n_controls)
    constraint = HasSufficientESS(min_ess=1000)
    assert _constraint_value(si, constraint) is False
