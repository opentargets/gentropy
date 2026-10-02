"""Test the fine-mapping constraint registry's fixed configuration."""

from __future__ import annotations

from pyspark.sql import Row, SparkSession

from gentropy.dataset.study_index import StudyIndex, StudyQualityCheck, StudyType
from gentropy.method.fine_mapping import FineMappingConstraintRegistry

STUDY_REQUIRED_SCHEMA = (
    "studyId STRING, projectId STRING, studyType STRING, "
    "hasSumstats BOOLEAN, qualityControls ARRAY<STRING>, analysisFlags ARRAY<STRING>, "
    "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>, "
    "traitFromSourceMappedIds ARRAY<STRING>, nSamples INT, nCases INT, nControls INT"
)


def _study_row(
    study_id: str,
    ld_population_structure: list[tuple[str, float]],
    n_samples: int | None = 100_000,
    quality_controls: list[str] | None = None,
    analysis_flags: list[str] | None = None,
) -> tuple[object, ...]:
    """Build a single StudyIndex row matching STUDY_REQUIRED_SCHEMA."""
    return (
        study_id,
        "p",
        StudyType.GWAS.value,
        True,
        quality_controls or [],
        analysis_flags or [],
        ld_population_structure,
        ["EFO_1"],
        n_samples,
        None,
        None,
    )


def _resolve_eligibility(spark: SparkSession, row: tuple[object, ...]) -> Row:
    """Resolve a single-row StudyIndex through the registered MultiSuSiE constraint set."""
    si = StudyIndex(_df=spark.createDataFrame([row], STUDY_REQUIRED_SCHEMA))
    constraint_set = FineMappingConstraintRegistry().registry["MultiSuSiE"]
    return constraint_set.resolve(si).df.collect()[0]


def _constraint_flag(row: Row, name: str) -> bool:
    """Look up a named constraint's value from a resolved plan row."""
    return next(c["value"] for c in row["constraints"] if c["name"] == name)


def test_registry_registers_multi_susie(spark: SparkSession) -> None:
    """The registry exposes exactly one constraint set, keyed "MultiSuSiE"."""
    registry = FineMappingConstraintRegistry().registry
    assert set(registry) == {"MultiSuSiE"}


def test_multi_susie_allows_eas_ancestry(spark: SparkSession) -> None:
    """EAS is an allowed major ancestry for the registered MultiSuSiE constraint set."""
    row = _study_row("s1", [("eas", 1.0)])
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "hasAllowedMajorAncestry") is True


def test_multi_susie_disallows_csa_ancestry(spark: SparkSession) -> None:
    """CSA is not an allowed major ancestry for the registered MultiSuSiE constraint set."""
    row = _study_row("s1", [("csa", 1.0)])
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "hasAllowedMajorAncestry") is False


def test_multi_susie_disallows_case_case_study_flag(spark: SparkSession) -> None:
    """A case-case study is excluded by the registered analysis-flag denylist."""
    row = _study_row("s1", [("nfe", 1.0)], analysis_flags=["Case-case study"])
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "hasAllowedAnalysisFlags") is False


def test_multi_susie_disallows_failed_gc_lambda_check(spark: SparkSession) -> None:
    """A study failing the GC lambda QC check is excluded by the registered denylist."""
    row = _study_row(
        "s1",
        [("nfe", 1.0)],
        quality_controls=["The GC lambda value is not within the expected range"],
    )
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "passSumstatQC") is False


def test_multi_susie_disallows_sumstats_not_available(spark: SparkSession) -> None:
    """A study whose sumstats are not available is excluded by passSumstatQC too, not only hasSumstats."""
    row = _study_row(
        "s1",
        [("nfe", 1.0)],
        quality_controls=["Harmonized summary statistics are not available or empty"],
    )
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "passSumstatQC") is False


def test_multi_susie_relative_sample_size_threshold_is_strict(
    spark: SparkSession,
) -> None:
    """The registered relative-sample-size threshold (0.95) rejects a slightly lower majority ancestry."""
    row = _study_row("s1", [("nfe", 0.9), ("afr", 0.1)])
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "hasAllowedMajorAncestry") is False


def test_registry_default_min_ess_rejects_sub_threshold_study(
    spark: SparkSession,
) -> None:
    """A measurement study below the default minimum ESS fails hasSufficientESS for the registered MultiSuSiE constraint set."""
    row = _study_row(
        "s1",
        [("nfe", 1.0)],
        n_samples=500,
        quality_controls=[StudyQualityCheck.MEASUREMENT_STUDY_DESIGN.value],
    )
    result = _resolve_eligibility(spark, row)
    assert _constraint_flag(result, "hasSufficientESS") is False


def test_registry_min_ess_is_configurable(spark: SparkSession) -> None:
    """A study below the default minimum ESS passes hasSufficientESS when the registry is configured with a lower threshold."""
    row = _study_row(
        "s1",
        [("nfe", 1.0)],
        n_samples=500,
        quality_controls=[StudyQualityCheck.MEASUREMENT_STUDY_DESIGN.value],
    )
    si = StudyIndex(_df=spark.createDataFrame([row], STUDY_REQUIRED_SCHEMA))
    constraint_set = FineMappingConstraintRegistry(min_ess=100).registry["MultiSuSiE"]
    result = constraint_set.resolve(si).df.collect()[0]
    assert _constraint_flag(result, "hasSufficientESS") is True
