"""Tests for annotating study loci with statistics and LD."""

from __future__ import annotations

import dbldatagen as dg
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import (
    AnnotatedStudyLocus,
    LDAnnotatedStudyLocus,
    StudyLocus,
    StudyLocusQualityCheck,
)
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.annotation import annotate_ld, annotate_locus

UNRESOLVED_LD = StudyLocusQualityCheck.UNRESOLVED_LD.value
NO_POPULATION = StudyLocusQualityCheck.NO_POPULATION.value


def _loci(
    spark: SparkSession, rows: list[tuple[str, str, int, int, str, list[str]]]
) -> StudyLocus:
    """Study loci of GCST1 from (id, chromosome, start, end, lead, flags).

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, str, int, int, str, list[str]]]): One tuple per row.

    Returns:
        StudyLocus: The loci.
    """
    return StudyLocus(
        _df=spark.createDataFrame(
            [
                (id_, "GCST1", chrom, start, end, lead, flags)
                for id_, chrom, start, end, lead, flags in rows
            ],
            StudyLocus.get_schema(),
        ),
        _schema=StudyLocus.get_schema(),
    )


def _summary_statistics(
    spark: SparkSession, rows: list[tuple[str, str, int]]
) -> SummaryStatistics:
    """Summary statistics from (studyId, chromosome, position), p-value 1e-10.

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, str, int]]): One tuple per variant.

    Returns:
        SummaryStatistics: The variants, named ``<chromosome>_<position>_A_T``.
    """
    return SummaryStatistics(
        _df=spark.createDataFrame(
            [
                (
                    study,
                    f"{chrom}_{pos}_A_T",
                    chrom,
                    pos,
                    0.1,
                    5000,
                    1.0,
                    -10,
                    0.3,
                    0.01,
                )
                for study, chrom, pos in rows
            ],
            SummaryStatistics.get_schema(),
        ),
        _schema=SummaryStatistics.get_schema(),
    )


def test_annotate_collects_the_study_associations_inside_the_bounds_by_position(
    spark: SparkSession,
) -> None:
    """Bounds are inclusive; other chromosomes and studies are left out."""
    loci = _loci(spark, [("L1", "1", 100, 300, "1_200_A_T", [])])
    summary_statistics = _summary_statistics(
        spark,
        [
            ("GCST1", "1", 300),
            ("GCST1", "1", 50),
            ("GCST1", "1", 250),
            ("GCST1", "1", 100),
            ("GCST1", "1", 301),
            ("GCST1", "2", 200),
            ("GCST2", "1", 200),
        ],
    )

    annotated = annotate_locus(loci, summary_statistics)

    assert isinstance(annotated, AnnotatedStudyLocus)
    (row,) = annotated.df.collect()
    assert [entry.variantId for entry in row.locus] == [
        "1_100_A_T",
        "1_250_A_T",
        "1_300_A_T",
    ]
    assert (row.id, row.leadVariantId, row.qualityControls) == ("L1", "1_200_A_T", [])


def test_annotate_gives_an_empty_locus_when_nothing_is_inside(
    spark: SparkSession,
) -> None:
    """Annotated with nothing found is an empty array, never null."""
    loci = _loci(spark, [("L1", "1", 100, 300, "1_200_A_T", [])])

    annotated = annotate_locus(loci, _summary_statistics(spark, [("GCST1", "1", 5000)]))

    assert annotated.df.select("locus").first() == ([],)


def test_rows_sharing_an_id_collect_each_association_once(
    spark: SparkSession,
) -> None:
    """A lead and its clumped variant share a locus; its entries are not doubled."""
    flag = StudyLocusQualityCheck.WINDOW_CLUMPED.value
    loci = _loci(
        spark,
        [
            ("L1", "1", 100, 300, "1_200_A_T", []),
            ("L1", "1", 100, 300, "1_150_A_T", [flag]),
        ],
    )
    summary_statistics = _summary_statistics(
        spark, [("GCST1", "1", 150), ("GCST1", "1", 200)]
    )

    annotated = annotate_locus(loci, summary_statistics)

    assert sorted(
        (row.leadVariantId, len(row.locus)) for row in annotated.df.collect()
    ) == [("1_150_A_T", 2), ("1_200_A_T", 2)]


def _annotated_loci(
    spark: SparkSession, rows: list[tuple[str, str, str]]
) -> AnnotatedStudyLocus:
    """Annotated loci on chromosome 1 from (id, studyId, lead), with an empty locus.

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, str, str]]): One tuple per locus.

    Returns:
        AnnotatedStudyLocus: The loci.
    """
    return AnnotatedStudyLocus(
        _df=spark.createDataFrame(
            [(id_, study, "1", 100, 300, lead, [], []) for id_, study, lead in rows],
            AnnotatedStudyLocus.get_schema(),
        ),
        _schema=AnnotatedStudyLocus.get_schema(),
    )


def _studies(
    spark: SparkSession, rows: list[tuple[str, list[tuple[str, float]] | None]]
) -> StudyIndex:
    """Studies from (studyId, [(population, relative sample size)] or None).

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, list[tuple[str, float]] | None]]): One tuple per study.

    Returns:
        StudyIndex: The studies, with placeholder project and study type.
    """
    return StudyIndex(
        _df=spark.createDataFrame(
            [(study, "PROJECT", "gwas", structure) for study, structure in rows],
            "studyId string, projectId string, studyType string, "
            "ldPopulationStructure array<struct<ldPopulation: string, "
            "relativeSampleSize: double>>",
        ),
        _schema=StudyIndex.get_schema(),
    )


def _ld_index(
    spark: SparkSession,
    rows: list[tuple[str, list[tuple[str, list[tuple[str, float]]]]]],
) -> LDIndex:
    """LD index on chromosome 1 from (variantId, [(tag, [(population, r)])]).

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, list[tuple[str, list[tuple[str, float]]]]]]): One
            tuple per panel variant.

    Returns:
        LDIndex: The index.
    """
    return LDIndex(
        _df=spark.createDataFrame(
            [(variant, "1", ld_set) for variant, ld_set in rows], LDIndex.get_schema()
        ),
        _schema=LDIndex.get_schema(),
    )


def _ld_sets(
    annotated: LDAnnotatedStudyLocus,
) -> dict[str, tuple[list[tuple[str, float]], list[str]]]:
    """Map each locus id to its (ldSet as (tag, r2) pairs, flags).

    Args:
        annotated (LDAnnotatedStudyLocus): The LD annotated loci.

    Returns:
        dict[str, tuple[list[tuple[str, float]], list[str]]]: Tags with r2 rounded
            to 4 decimals, and the sorted flags.
    """
    return {
        row.id: (
            [(tag.tagVariantId, round(tag.r2Overall, 4)) for tag in row.ldSet],
            sorted(row.qualityControls),
        )
        for row in annotated.df.collect()
    }


LEAD = "1_200_A_T"
PANEL = [
    (
        LEAD,
        [
            (LEAD, [("nfe", 1.0), ("afr", 1.0)]),
            ("1_210_C_G", [("nfe", 0.9), ("afr", -0.5)]),
            ("1_220_G_A", [("nfe", 0.7)]),
        ],
    )
]


def test_annotate_ld_squares_r_of_the_studys_major_population(
    spark: SparkSession,
) -> None:
    """r2 is read from the largest population; a tag it lacks counts as r2 0."""
    loci = _annotated_loci(spark, [("L1", "AFR", LEAD), ("L2", "NFE", LEAD)])
    studies = _studies(
        spark, [("AFR", [("nfe", 0.2), ("afr", 0.8)]), ("NFE", [("nfe", 1.0)])]
    )

    annotated = annotate_ld(loci, studies, _ld_index(spark, PANEL), r2_threshold=0.2)

    assert isinstance(annotated, LDAnnotatedStudyLocus)
    assert _ld_sets(annotated) == {
        "L1": ([(LEAD, 1.0), ("1_210_C_G", 0.25)], []),
        "L2": ([(LEAD, 1.0), ("1_210_C_G", 0.81), ("1_220_G_A", 0.49)], []),
    }


def test_annotate_ld_keeps_tags_at_the_threshold(spark: SparkSession) -> None:
    """The threshold is inclusive: r2 0.49 stays at 0.49 and goes above it."""
    loci = _annotated_loci(spark, [("L1", "NFE", LEAD)])
    studies = _studies(spark, [("NFE", [("nfe", 1.0)])])
    ld_index = _ld_index(spark, PANEL)

    at = _ld_sets(annotate_ld(loci, studies, ld_index, r2_threshold=0.49))
    above = _ld_sets(annotate_ld(loci, studies, ld_index, r2_threshold=0.5))

    assert [tag for tag, _ in at["L1"][0]] == [LEAD, "1_210_C_G", "1_220_G_A"]
    assert [tag for tag, _ in above["L1"][0]] == [LEAD, "1_210_C_G"]


@pytest.mark.parametrize(
    ("structure", "major"),
    [
        ([("afr", 0.5), ("nfe", 0.5)], "nfe"),
        ([("eas", 0.5), ("afr", 0.5)], "afr"),
    ],
    ids=["tie-with-nfe-picks-nfe", "tie-without-nfe-picks-first-by-name"],
)
def test_annotate_ld_breaks_population_ties_deterministically(
    spark: SparkSession, structure: list[tuple[str, float]], major: str
) -> None:
    """A tie goes to nfe when it is among the largest, else to the first by name."""
    panel = [(LEAD, [(LEAD, [(major, 1.0)]), ("1_210_C_G", [(major, 0.9)])])]
    loci = _annotated_loci(spark, [("L1", "S", LEAD)])

    annotated = annotate_ld(
        loci, _studies(spark, [("S", structure)]), _ld_index(spark, panel)
    )

    assert _ld_sets(annotated)["L1"][0] == [(LEAD, 1.0), ("1_210_C_G", 0.81)]


def test_annotate_ld_rescues_a_lead_missing_from_the_panel(
    spark: SparkSession,
) -> None:
    """A lead the panel does not know tags only itself and is flagged unresolved."""
    loci = _annotated_loci(spark, [("L1", "NFE", "1_999_A_T")])
    studies = _studies(spark, [("NFE", [("nfe", 1.0)])])

    annotated = annotate_ld(loci, studies, _ld_index(spark, PANEL))

    assert _ld_sets(annotated) == {"L1": ([("1_999_A_T", 1.0)], [UNRESOLVED_LD])}


def test_annotate_ld_flags_a_study_without_population(spark: SparkSession) -> None:
    """With no ancestry there is no population to read, so LD is unresolved too."""
    loci = _annotated_loci(spark, [("L1", "UNKNOWN", LEAD)])
    studies = _studies(spark, [("UNKNOWN", None)])

    annotated = annotate_ld(loci, studies, _ld_index(spark, PANEL))

    assert _ld_sets(annotated) == {
        "L1": ([(LEAD, 1.0)], sorted([NO_POPULATION, UNRESOLVED_LD]))
    }


def test_annotate_ld_keeps_the_locus(spark: SparkSession) -> None:
    """LD annotation adds to an annotated locus; it never drops ``locus``."""
    loci = _annotated_loci(spark, [("L1", "NFE", LEAD)])
    studies = _studies(spark, [("NFE", [("nfe", 1.0)])])

    annotated = annotate_ld(loci, studies, _ld_index(spark, PANEL))

    assert annotated.df.columns == LDAnnotatedStudyLocus.get_schema().fieldNames()
    assert annotated.df.select("locus").first() == ([],)


@pytest.mark.parametrize(
    ("r_values", "expected"),
    [
        ([("pop1", 0.5), ("pop2", 0.6)], 0.25),
        ([("pop2", 0.6)], 0.0),
    ],
    ids=["r-of-major-population", "major-population-missing"],
)
def test_annotate_ld_r2_is_r_squared_in_the_major_population(
    spark: SparkSession, r_values: list[tuple[str, float]], expected: float
) -> None:
    """r2Overall is r of the major population squared, and 0 when it has no r."""
    panel = [(LEAD, [(LEAD, [("pop1", 1.0)]), ("tag1", r_values)])]
    loci = _annotated_loci(spark, [("L1", "study1", LEAD)])
    studies = _studies(spark, [("study1", [("pop1", 0.8), ("pop3", 0.2)])])

    annotated = annotate_ld(loci, studies, _ld_index(spark, panel), r2_threshold=0.0)

    assert _ld_sets(annotated)["L1"][0] == [(LEAD, 1.0), ("tag1", expected)]


def _mandatory(schema: t.StructType, *optional: str) -> t.StructType:
    """Keep the mandatory columns of a dataset schema, plus the named optional ones.

    Args:
        schema (t.StructType): The dataset schema.
        *optional (str): Optional columns to keep as well.

    Returns:
        t.StructType: The kept columns, in schema order.
    """
    return t.StructType(
        [field for field in schema if not field.nullable or field.name in optional]
    )


@pytest.fixture()
def random_ld_index(spark: SparkSession) -> LDIndex:
    """Random LD index on chromosome 1, keyed by the row number as variant id.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        LDIndex: 400 panel variants, each tagging one random variant.
    """
    data_spec = (
        dg.DataGenerator(
            spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
        )
        .withSchema(LDIndex.get_schema())
        .withColumnSpec("variantId", expr="cast(id as string)")
        .withColumnSpec("chromosome", expr="'1'")
        .withColumnSpec(
            "ldSet",
            expr="array(named_struct('tagVariantId', cast(rand() as string), "
            "'rValues', array(named_struct('population', cast(rand() as string), "
            "'r', cast(rand() as double)))))",
        )
    )
    return LDIndex(_df=data_spec.build(), _schema=LDIndex.get_schema())


@pytest.fixture()
def random_study_index(spark: SparkSession) -> StudyIndex:
    """Random study index with its mandatory columns, keyed by the row number.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        StudyIndex: 400 studies, a tenth without population structure.
    """
    data_spec = (
        dg.DataGenerator(
            spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
        )
        .withSchema(
            _mandatory(
                StudyIndex.get_schema(), "ldPopulationStructure", "qualityControls"
            )
        )
        .withColumnSpec("studyId", expr="cast(id as string)")
        .withColumnSpec(
            "ldPopulationStructure",
            expr="array(named_struct('ldPopulation', cast(rand() as string), "
            "'relativeSampleSize', rand()))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "qualityControls", expr="array(cast(rand() as string))", percentNulls=0.1
        )
    )
    return StudyIndex(_df=data_spec.build(), _schema=StudyIndex.get_schema())


@pytest.fixture()
def random_annotated_study_locus(spark: SparkSession) -> AnnotatedStudyLocus:
    """Random annotated study loci on chromosome 1, one per study of the random index.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        AnnotatedStudyLocus: 400 loci whose lead is the LD index variant of the
            same row number.
    """
    # The seed column is renamed, as ``id`` is the study locus key.
    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            seedColumnName="row",
        )
        .withSchema(_mandatory(AnnotatedStudyLocus.get_schema()))
        .withColumnSpec("id", expr="cast(row as string)")
        .withColumnSpec("studyId", expr="cast(row as string)")
        .withColumnSpec("chromosome", expr="'1'")
        .withColumnSpec("leadVariantId", expr="cast(row as string)", percentNulls=0.1)
        .withColumnSpec(
            "qualityControls", expr="array(cast(rand() as string))", percentNulls=0.1
        )
        .withColumnSpec(
            "locus",
            expr="array(named_struct('variantId', cast(row as string), "
            "'pValueMantissa', cast(rand() as float), 'pValueExponent', -8, "
            "'effectAlleleFrequencyFromSource', cast(rand() as float), "
            "'sampleSize', 1000, 'beta', rand(), 'standardError', rand()))",
        )
    )
    return AnnotatedStudyLocus(
        _df=data_spec.build(), _schema=AnnotatedStudyLocus.get_schema()
    )


def test_annotate_ld_on_random_data(
    random_annotated_study_locus: AnnotatedStudyLocus,
    random_study_index: StudyIndex,
    random_ld_index: LDIndex,
) -> None:
    """Random loci, studies and panel annotate into one LD set per locus."""
    annotated = annotate_ld(
        random_annotated_study_locus, random_study_index, random_ld_index
    )

    assert isinstance(annotated, LDAnnotatedStudyLocus)
    rows = annotated.df.collect()
    assert len(rows) == random_annotated_study_locus.df.count()
    assert all(row.ldSet is not None for row in rows if row.leadVariantId is not None)
