"""Test fine-mapping method constraint sets."""

from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession

from gentropy.common.types import LDPopulation
from gentropy.dataset.fine_mapping import FineMappingPlanner, FineMappingRoute
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.method.fine_mapping.constraint_set import MultiSuSiEConstraintSet

STUDY_REQUIRED_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, "
    "hasSumstats BOOLEAN, qualityControls ARRAY<STRING>, analysisFlags ARRAY<STRING>, "
    "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>, "
    "traitFromSourceMappedIds ARRAY<STRING>, nSamples INT, nCases INT, nControls INT"
)


def _study_row(
    study_id: str,
    trait_ids: list[str],
    ld_population_structure: list[tuple[str, float]],
    n_samples: int | None = 1000,
    n_cases: int | None = None,
    n_controls: int | None = None,
    study_type: str = StudyType.GWAS.value,
    has_sumstats: bool = True,
    quality_controls: list[str] | None = None,
    analysis_flags: list[str] | None = None,
) -> tuple[object, ...]:
    """Build a single StudyIndex row matching STUDY_REQUIRED_SCHEMA."""
    return (
        study_id,
        "p",
        study_type,
        has_sumstats,
        quality_controls or [],
        analysis_flags or [],
        ld_population_structure,
        trait_ids,
        n_samples,
        n_cases,
        n_controls,
    )


def _constraint_set(
    allowed_ancestries: list[LDPopulation] | None = None,
    relative_sample_size_threshold: float = 0.5,
) -> MultiSuSiEConstraintSet:
    """Build a MultiSuSiEConstraintSet with permissive defaults (nothing disallowed)."""
    return MultiSuSiEConstraintSet(
        allowed_ancestries=allowed_ancestries or [LDPopulation.NFE, LDPopulation.AFR],
        relative_sample_size_threshold=relative_sample_size_threshold,
        disallowed_reasons=[],
        disallowed_flags=[],
    )


def _resolve(spark: SparkSession, rows: list[tuple[object, ...]]) -> FineMappingPlanner:
    """Build a StudyIndex from the given rows and resolve it with a permissive MultiSuSiEConstraintSet."""
    si = StudyIndex(_df=spark.createDataFrame(rows, STUDY_REQUIRED_SCHEMA))
    return _constraint_set().resolve(si)


def _constraint_flag(row: Row, name: str) -> bool:
    """Look up a named constraint's value from a resolved plan row."""
    return next(c["value"] for c in row["constraints"] if c["name"] == name)


def test_resolve_returns_fine_mapping_planner(spark: SparkSession) -> None:
    """resolve() returns a FineMappingPlanner with the expected schema."""
    rows = [_study_row("s1", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)])]
    result = _resolve(spark, rows)
    assert isinstance(result, FineMappingPlanner)
    assert result.df.columns == ["studyId", "runId", "constraints", "route"]


def test_resolve_single_eligible_study_has_no_run_id(spark: SparkSession) -> None:
    """A lone eligible study for a trait is its own representative but has no cross-ancestry counterpart."""
    rows = [_study_row("s1", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)])]
    result = _resolve(spark, rows)
    row = result.df.collect()[0]
    assert row["studyId"] == "s1"
    assert row["runId"] is None
    assert row["route"] == FineMappingRoute.MULTI_SUSIE_ROUTE.value
    assert _constraint_flag(row, "representativeStudy") is True
    assert _constraint_flag(row, "hasOtherAncestryCounterpart") is False


def test_resolve_groups_multi_ancestry_studies_under_shared_run_id(
    spark: SparkSession,
) -> None:
    """Two eligible studies of the same trait but different major ancestries share one runId."""
    rows = [
        _study_row("s1", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)]),
        _study_row("s2", ["EFO_1"], [(LDPopulation.AFR.value, 1.0)]),
    ]
    result = _resolve(spark, rows)
    rows_by_id = {r["studyId"]: r for r in result.df.collect()}
    assert set(rows_by_id) == {"s1", "s2"}
    assert rows_by_id["s1"]["runId"] is not None
    assert rows_by_id["s1"]["runId"] == rows_by_id["s2"]["runId"]
    for row in rows_by_id.values():
        assert _constraint_flag(row, "representativeStudy") is True
        assert _constraint_flag(row, "hasOtherAncestryCounterpart") is True


def test_resolve_same_ancestry_keeps_only_higher_n_eff_as_representative(
    spark: SparkSession,
) -> None:
    """Among two studies sharing trait and major ancestry, only the higher-n_eff one is representative."""
    rows = [
        _study_row("small", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)], n_samples=100),
        _study_row(
            "large", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)], n_samples=10_000
        ),
    ]
    result = _resolve(spark, rows)
    rows_by_id = {r["studyId"]: r for r in result.df.collect()}
    assert _constraint_flag(rows_by_id["large"], "representativeStudy") is True
    assert _constraint_flag(rows_by_id["small"], "representativeStudy") is False
    # Only one representative study per (trait, ancestry) group -> no multi-ancestry counterpart.
    assert _constraint_flag(rows_by_id["large"], "hasOtherAncestryCounterpart") is False


def test_resolve_ineligible_study_is_never_representative(
    spark: SparkSession,
) -> None:
    """An ineligible study (e.g. no sumstats) is never marked representativeStudy, even as the sole ineligible study for its trait/ancestry group."""
    rows = [
        _study_row(
            "no_sumstats",
            ["EFO_1"],
            [(LDPopulation.NFE.value, 1.0)],
            n_samples=100_000,
            has_sumstats=False,
        ),
        _study_row(
            "eligible", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)], n_samples=100
        ),
    ]
    result = _resolve(spark, rows)
    rows_by_id = {r["studyId"]: r for r in result.df.collect()}
    assert _constraint_flag(rows_by_id["eligible"], "representativeStudy") is True
    assert _constraint_flag(rows_by_id["no_sumstats"], "hasSumstats") is False
    assert _constraint_flag(rows_by_id["no_sumstats"], "representativeStudy") is False


def test_resolve_case_control_uses_effective_sample_size(spark: SparkSession) -> None:
    """A case-control study with a lower nSamples but higher effective sample size wins as representative."""
    rows = [
        # Effective sample size = 4*5000*5000/(5000+5000) = 10_000, higher than "measurement" study's 8_000.
        _study_row(
            "case_control",
            ["EFO_1"],
            [(LDPopulation.NFE.value, 1.0)],
            n_samples=10_000,
            n_cases=5_000,
            n_controls=5_000,
        ),
        _study_row(
            "measurement", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)], n_samples=8_000
        ),
    ]
    result = _resolve(spark, rows)
    rows_by_id = {r["studyId"]: r for r in result.df.collect()}
    assert _constraint_flag(rows_by_id["case_control"], "representativeStudy") is True
    assert _constraint_flag(rows_by_id["measurement"], "representativeStudy") is False


def test_resolve_undetermined_study_design_is_never_representative(
    spark: SparkSession,
) -> None:
    """A study whose design is neither case-control nor measurement (n_eff undetermined) is never representative, even as the sole study for its trait/ancestry group."""
    rows = [
        # Only nCases populated (nControls null) -> validate_ccs() flags
        # ONE_ONLY_CASE_OR_CONTROL, neither CASE_CONTROL_STUDY_DESIGN nor
        # MEASUREMENT_STUDY_DESIGN, so n_eff is null.
        _study_row(
            "s1",
            ["EFO_1"],
            [(LDPopulation.NFE.value, 1.0)],
            n_samples=10_000,
            n_cases=5_000,
            n_controls=None,
        ),
    ]
    result = _resolve(spark, rows)
    row = result.df.collect()[0]
    assert _constraint_flag(row, "representativeStudy") is False


def test_resolve_different_traits_are_independent(spark: SparkSession) -> None:
    """Studies mapped to different traits never share a runId even with matching ancestries."""
    rows = [
        _study_row("s1", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)]),
        _study_row("s2", ["EFO_2"], [(LDPopulation.AFR.value, 1.0)]),
    ]
    result = _resolve(spark, rows)
    rows_by_id = {r["studyId"]: r for r in result.df.collect()}
    assert rows_by_id["s1"]["runId"] is None
    assert rows_by_id["s2"]["runId"] is None
    for row in rows_by_id.values():
        assert _constraint_flag(row, "hasOtherAncestryCounterpart") is False


def test_resolve_missing_mapped_trait_is_flagged_ineligible(
    spark: SparkSession,
) -> None:
    """A study with no mapped trait fails the HasMappedTrait constraint."""
    rows = [_study_row("s1", [], [(LDPopulation.NFE.value, 1.0)])]
    result = _resolve(spark, rows)
    row = result.df.collect()[0]
    assert _constraint_flag(row, "hasMappedTrait") is False


def test_resolve_no_representative_when_all_studies_ineligible(
    spark: SparkSession,
) -> None:
    """When every study for a trait/ancestry group is ineligible, none of them is marked representativeStudy."""
    rows = [
        _study_row(
            "s1",
            ["EFO_1"],
            [(LDPopulation.NFE.value, 1.0)],
            n_samples=100,
            has_sumstats=False,
        ),
        _study_row(
            "s2",
            ["EFO_1"],
            [(LDPopulation.NFE.value, 1.0)],
            n_samples=200,
            has_sumstats=False,
        ),
    ]
    result = _resolve(spark, rows)
    for row in result.df.collect():
        assert _constraint_flag(row, "representativeStudy") is False


def test_resolve_preserves_distinct_study_count(spark: SparkSession) -> None:
    """resolve() never drops or duplicates studies: output distinct studyId count matches input."""
    rows = [
        _study_row("s1", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)]),
        _study_row("s2", ["EFO_1"], [(LDPopulation.AFR.value, 1.0)]),
        _study_row("s3", [], [(LDPopulation.NFE.value, 1.0)]),  # ineligible, kept
        _study_row(
            "s4", ["EFO_2"], [(LDPopulation.NFE.value, 1.0)], has_sumstats=False
        ),
    ]
    result = _resolve(spark, rows)
    assert result.df.select("studyId").distinct().count() == 4
    assert {r["studyId"] for r in result.df.collect()} == {"s1", "s2", "s3", "s4"}


def test_resolve_raises_when_study_count_invariant_is_broken(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """resolve() raises ValueError if a bug caused the output to lose (or duplicate) studies."""
    rows = [
        _study_row("s1", ["EFO_1"], [(LDPopulation.NFE.value, 1.0)]),
        _study_row("s2", ["EFO_1"], [(LDPopulation.AFR.value, 1.0)]),
    ]
    si = StudyIndex(_df=spark.createDataFrame(rows, STUDY_REQUIRED_SCHEMA))
    constraint_set = _constraint_set()

    original_assign_run_id = MultiSuSiEConstraintSet._assign_run_id
    monkeypatch.setattr(
        MultiSuSiEConstraintSet,
        "_assign_run_id",
        staticmethod(lambda df: original_assign_run_id(df.filter("studyId != 's2'"))),
    )

    with pytest.raises(ValueError, match="distinct studies"):
        constraint_set.resolve(si)
