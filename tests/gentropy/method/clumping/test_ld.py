"""Tests for LD clumping."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import (
    AnnotatedStudyLocus,
    LDAnnotatedStudyLocus,
    StudyLocusQualityCheck,
)
from gentropy.method.annotation import annotate_ld
from gentropy.method.clumping.ld import LDClumping

LD_CLUMPED = StudyLocusQualityCheck.LD_CLUMPED.value


def _loci(
    spark: SparkSession,
    rows: list[tuple[str, str, str, int, list[str]]],
) -> LDAnnotatedStudyLocus:
    """LD annotated loci from (studyId, chromosome, lead, p-value exponent, tags).

    Each lead's ``locus`` holds the lead itself with mantissa 1.0 and the given
    exponent, so the exponent orders the leads. Its ``ldSet`` is the lead plus
    the tags, all at r2 1.0. The id is the lead.

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, str, str, int, list[str]]]): One tuple per locus.

    Returns:
        LDAnnotatedStudyLocus: The loci, with no flags.
    """
    return LDAnnotatedStudyLocus(
        _df=spark.createDataFrame(
            [
                (
                    lead,
                    study,
                    chromosome,
                    1,
                    1000,
                    lead,
                    [],
                    [(lead, 1.0, exponent, 0.3, 1000, 0.1, 0.01)],
                    [(tag, 1.0) for tag in [lead, *tags]],
                )
                for study, chromosome, lead, exponent, tags in rows
            ],
            LDAnnotatedStudyLocus.get_schema(),
        ),
        _schema=LDAnnotatedStudyLocus.get_schema(),
    )


def _clumped(loci: LDAnnotatedStudyLocus) -> dict[str, tuple[int, list[str]]]:
    """Map each lead to the size of its ``ldSet`` and its flags.

    Args:
        loci (LDAnnotatedStudyLocus): Clumped loci.

    Returns:
        dict[str, tuple[int, list[str]]]: ``ldSet`` size and flags per lead.
    """
    return {
        row.leadVariantId: (len(row.ldSet), row.qualityControls)
        for row in loci.df.collect()
    }


def test_a_lead_tagging_a_more_significant_lead_is_clumped(
    spark: SparkSession,
) -> None:
    """B tags the more significant A, so B is flagged and its ``ldSet`` emptied."""
    loci = _loci(spark, [("S", "1", "A", -20, ["B"]), ("S", "1", "B", -10, ["A"])])

    clumped = LDClumping(keep_clumped_loci=True).clump(loci)

    assert isinstance(clumped, LDAnnotatedStudyLocus)
    assert _clumped(clumped) == {"A": (2, []), "B": (0, [LD_CLUMPED])}


def test_a_clumped_lead_still_clumps_the_leads_it_tags(
    spark: SparkSession,
) -> None:
    """Dev's rule: B, though clumped by A, clumps C, which A does not tag."""
    loci = _loci(
        spark,
        [
            ("S", "1", "A", -30, ["B"]),
            ("S", "1", "B", -20, ["A", "C"]),
            ("S", "1", "C", -10, ["B"]),
        ],
    )

    clumped = LDClumping(keep_clumped_loci=True).clump(loci)

    assert _clumped(clumped) == {
        "A": (2, []),
        "B": (0, [LD_CLUMPED]),
        "C": (0, [LD_CLUMPED]),
    }


def test_clumped_loci_are_dropped_by_default(spark: SparkSession) -> None:
    """Without ``keep_clumped_loci`` only the leads that survive are returned."""
    loci = _loci(spark, [("S", "1", "A", -20, ["B"]), ("S", "1", "B", -10, ["A"])])

    clumped = LDClumping().clump(loci)

    assert _clumped(clumped) == {"A": (2, [])}


def test_leads_of_other_studies_or_chromosomes_never_clump(
    spark: SparkSession,
) -> None:
    """Linkage is looked for within one study and one chromosome."""
    loci = _loci(
        spark,
        [
            ("S", "1", "A", -20, ["B", "C"]),
            ("T", "1", "B", -10, ["A"]),
            ("S", "2", "C", -10, ["A"]),
        ],
    )

    clumped = LDClumping().clump(loci)

    assert sorted(_clumped(clumped)) == ["A", "B", "C"]


def test_equal_p_values_are_ordered_by_lead(spark: SparkSession) -> None:
    """A tie goes to the first lead by id, so the same input clumps the same way."""
    loci = _loci(spark, [("S", "1", "B", -10, ["A"]), ("S", "1", "A", -10, ["B"])])

    clumped = LDClumping().clump(loci)

    assert sorted(_clumped(clumped)) == ["A"]


def test_a_lead_missing_from_its_locus_is_least_significant(
    spark: SparkSession,
) -> None:
    """Without its own p-value a lead can be clumped but clumps nothing."""
    loci = _loci(spark, [("S", "1", "A", -10, ["B"]), ("S", "1", "B", -20, ["A"])])
    schema = LDAnnotatedStudyLocus.get_schema()
    stripped = LDAnnotatedStudyLocus(
        _df=loci.df.withColumn(
            "locus",
            f.when(
                f.col("leadVariantId") == "B",
                f.array().cast(schema["locus"].dataType),
            ).otherwise(f.col("locus")),
        ),
        _schema=schema,
    )

    clumped = LDClumping().clump(stripped)

    assert sorted(_clumped(clumped)) == ["A"]


def _shuffled_columns(loci: LDAnnotatedStudyLocus) -> list[str]:
    """The input columns of every shuffle in the plan, from Spark's formatted plan.

    Args:
        loci (LDAnnotatedStudyLocus): The dataset whose plan to read.

    Returns:
        list[str]: One ``Input [...]`` line per ``Exchange`` node.
    """
    jvm = loci.df.sparkSession.sparkContext._jvm
    assert jvm is not None
    plan = jvm.PythonSQLUtils.explainString(loci.df._jdf.queryExecution(), "formatted")
    nodes = [node.strip().splitlines() for node in plan.split("\n\n")]
    return [
        line
        for node in nodes
        if node and "Exchange" in node[0]
        for line in node
        if line.startswith("Input")
    ]


@pytest.mark.parametrize("keep_clumped_loci", [False, True])
def test_locus_is_never_shuffled(spark: SparkSession, keep_clumped_loci: bool) -> None:
    """Linkage is found on a narrow projection; the wide rows never move."""
    loci = _loci(spark, [("S", "1", "A", -20, ["B"]), ("S", "1", "B", -10, ["A"])])

    clumped = LDClumping(keep_clumped_loci=keep_clumped_loci).clump(loci)

    shuffled = _shuffled_columns(clumped)
    assert shuffled, "the plan should shuffle the narrow projection"
    assert not [line for line in shuffled if "locus" in line]


def test_clump_on_random_data(
    random_annotated_study_locus: AnnotatedStudyLocus,
    random_study_index: StudyIndex,
    random_ld_index: LDIndex,
) -> None:
    """Random LD annotated loci clump into the same dataset type."""
    loci = annotate_ld(
        random_annotated_study_locus, random_study_index, random_ld_index
    )

    clumped = LDClumping(keep_clumped_loci=True).clump(loci)

    assert isinstance(clumped, LDAnnotatedStudyLocus)
    assert clumped.df.count() == loci.df.count()
