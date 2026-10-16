"""Test locus overlap dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest

from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.locus_overlap import LocusOverlap
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import AnnotatedStudyLocus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_locus_overlap_creation(mock_locus_overlap: LocusOverlap) -> None:
    """Test locus overlap creation with mock data."""
    assert isinstance(mock_locus_overlap, LocusOverlap)


def test_convert_to_square_matrix(spark: SparkSession) -> None:
    """Test _convert_to_square_matrix."""
    mock_sl_overlap = LocusOverlap(
        _df=spark.createDataFrame(
            [
                ("1", "2", "eqtl", "CHROM1", "variant2", None),
            ],
            LocusOverlap.get_schema(),
        ),
        _schema=LocusOverlap.get_schema(),
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "2", "eqtl", "CHROM1", "variant2", None),
            ("2", "1", "eqtl", "CHROM1", "variant2", None),
        ],
        LocusOverlap.get_schema(),
    )
    observed_df = mock_sl_overlap._convert_to_square_matrix().df

    assert observed_df.collect() == expected_df.collect()


def _credible_set(id: str, study: str, study_type: str) -> tuple[Any, ...]:
    """A credible set row on chromosome 1.

    Args:
        id (str): Credible set id.
        study (str): Study id.
        study_type (str): Study type.

    Returns:
        tuple[Any, ...]: The row, in the CredibleSet schema order.
    """
    return (
        id,
        "1",
        1,
        100,
        study,
        study_type,
        "lead",
        "SuSiE",
        None,
        None,
        None,
        None,
        [],
    )


def _variant(
    id: str, variant: str, posterior: float, log_bf: float | None = None
) -> tuple[Any, ...]:
    """A credible set variant row.

    Args:
        id (str): Credible set id.
        variant (str): Variant id.
        posterior (float): Posterior probability.
        log_bf (float | None): Natural log Bayes factor.

    Returns:
        tuple[Any, ...]: The row, in the CredibleSetVariant schema order.
    """
    return (
        id,
        variant,
        1.0,
        -8,
        None,
        None,
        0.1,
        0.01,
        posterior,
        log_bf,
        None,
        None,
        None,
        None,
        None,
        None,
        [],
    )


@pytest.mark.parametrize(
    "has_overlap, expected",
    [
        # Overlap exists: every tag of both credible sets, aligned
        (
            True,
            [("commonTag", 0.9, 0.6), ("nonCommonTag", None, 0.6)],
        ),
        # No overlap
        (False, []),
    ],
)
def test_from_credible_sets_semantic(
    spark: SparkSession, has_overlap: bool, expected: list[Any]
) -> None:
    """Credible sets overlap through a shared variant, keeping all tags of both."""
    credible_sets = CredibleSet(
        spark.createDataFrame(
            [
                _credible_set("1", "study1", "gwas"),
                _credible_set("2", "study2", "eqtl"),
            ],
            CredibleSet.get_schema(),
        )
    )
    left_tag = "commonTag" if has_overlap else "var1"
    variants = CredibleSetVariant(
        spark.createDataFrame(
            [
                _variant("1", left_tag, 0.9),
                _variant("2", "commonTag", 0.6),
                _variant("2", "nonCommonTag", 0.6),
            ],
            CredibleSetVariant.get_schema(),
        )
    )

    overlaps = LocusOverlap.from_credible_sets(credible_sets, variants)

    def rounded(value: float | None) -> float | None:
        return None if value is None else round(value, 6)

    observed = sorted(
        (
            row.tagVariantId,
            rounded(row.left_posteriorProbability),
            rounded(row.right_posteriorProbability),
        )
        for row in overlaps.df.select(
            "tagVariantId",
            "statistics.left_posteriorProbability",
            "statistics.right_posteriorProbability",
        ).collect()
    )
    assert observed == expected
    if has_overlap:
        assert {
            (row.leftLocusId, row.rightLocusId) for row in overlaps.df.collect()
        } == {("1", "2")}


def test_from_credible_sets_statistics(spark: SparkSession) -> None:
    """Tags carry the natural log Bayes factor and the reported (marginal) beta."""
    credible_sets = CredibleSet(
        spark.createDataFrame(
            [
                _credible_set("1", "study1", "gwas"),
                _credible_set("2", "study2", "eqtl"),
            ],
            CredibleSet.get_schema(),
        )
    )
    variants = CredibleSetVariant(
        spark.createDataFrame(
            [_variant("1", "tag", 0.9, 2.0), _variant("2", "tag", 0.6, 3.0)],
            CredibleSetVariant.get_schema(),
        )
    )
    [row] = (
        LocusOverlap.from_credible_sets(credible_sets, variants)
        .df.select("statistics.*")
        .collect()
    )
    assert (row.left_logBF, row.right_logBF) == (2.0, 3.0)
    assert (row.left_beta, row.right_beta) == (0.1, 0.1)
    assert (row.left_pValueExponent, row.right_pValueExponent) == (-8, -8)


def test_from_study_loci(spark: SparkSession) -> None:
    """Study loci overlap through their locus, with the study type from the study index."""

    def locus(variant: str, beta: float) -> tuple[Any, ...]:
        return (variant, 1.0, -8, None, None, beta, 0.01)

    study_loci = AnnotatedStudyLocus(
        spark.createDataFrame(
            [
                ("L1", "study1", "1", 1, 100, "tag", [], [locus("tag", 0.2)], None),
                ("L2", "study2", "1", 1, 100, "tag", [], [locus("tag", -0.3)], None),
                # A study missing from the study index is left out:
                ("L3", "study3", "1", 1, 100, "tag", [], [locus("tag", 0.1)], None),
            ],
            AnnotatedStudyLocus.get_schema(),
        )
    )
    study_index = StudyIndex(
        spark.createDataFrame(
            [("study1", "GCST", "gwas"), ("study2", "GCST", "eqtl")],
            "studyId string, projectId string, studyType string",
        )
    )

    overlaps = LocusOverlap.from_study_loci(study_loci, study_index)

    [row] = overlaps.df.select(
        "leftLocusId", "rightLocusId", "rightStudyType", "statistics.*"
    ).collect()
    assert (row.leftLocusId, row.rightLocusId, row.rightStudyType) == (
        "L1",
        "L2",
        "eqtl",
    )
    assert (row.left_beta, row.right_beta) == (0.2, -0.3)
    assert (row.left_logBF, row.left_posteriorProbability) == (None, None)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "id": "default_mixed_gwas_qtl_and_gwas_gwas",
            "rows": [
                # Three loci sharing same variant V1 on chr1
                {
                    "locusId": "10",
                    "studyId": "GWAS1",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "V1",
                },
                {
                    "locusId": "05",
                    "studyId": "GWAS2",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "V1",
                },
                {
                    "locusId": "30",
                    "studyId": "EQTL1",
                    "studyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "V1",
                },
            ],
            # Expected:
            # GWAS-GWAS pair: left id must be larger (10 > 05) so (10,05)
            # GWAS-QTL pairs: both GWAS loci vs EQTL
            "expected": [
                {
                    "leftLocusId": "10",
                    "rightLocusId": "05",
                    "rightStudyType": "gwas",
                    "chromosome": "1",
                },
                {
                    "leftLocusId": "10",
                    "rightLocusId": "30",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                },
                {
                    "leftLocusId": "05",
                    "rightLocusId": "30",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                },
            ],
            "restrict_right_studies": None,
            "gwas_v_qtl_overlap_only": False,
        },
        {
            "id": "gwas_v_qtl_only",
            "rows": [
                {
                    "locusId": "10",
                    "studyId": "GWAS1",
                    "studyType": "gwas",
                    "chromosome": "2",
                    "tagVariantId": "X",
                },
                {
                    "locusId": "11",
                    "studyId": "GWAS2",
                    "studyType": "gwas",
                    "chromosome": "2",
                    "tagVariantId": "X",
                },
                {
                    "locusId": "20",
                    "studyId": "EQTL1",
                    "studyType": "eqtl",
                    "chromosome": "2",
                    "tagVariantId": "X",
                },
            ],
            # No GWAS-GWAS pair expected
            "expected": [
                {
                    "leftLocusId": "10",
                    "rightLocusId": "20",
                    "rightStudyType": "eqtl",
                    "chromosome": "2",
                },
                {
                    "leftLocusId": "11",
                    "rightLocusId": "20",
                    "rightStudyType": "eqtl",
                    "chromosome": "2",
                },
            ],
            "restrict_right_studies": None,
            "gwas_v_qtl_overlap_only": True,
        },
        {
            "id": "restrict_right_studies_qtl",
            "rows": [
                {
                    "locusId": "1",
                    "studyId": "GWAS_A",
                    "studyType": "gwas",
                    "chromosome": "3",
                    "tagVariantId": "Z",
                },
                {
                    "locusId": "2",
                    "studyId": "GWAS_B",
                    "studyType": "gwas",
                    "chromosome": "3",
                    "tagVariantId": "Z",
                },
                {
                    "locusId": "3",
                    "studyId": "EQTL_B",
                    "studyType": "eqtl",
                    "chromosome": "3",
                    "tagVariantId": "Z",
                },
            ],
            # Restrict right to EQTL_B only; left must not be in restrict list
            "expected": [
                {
                    "leftLocusId": "1",
                    "rightLocusId": "3",
                    "rightStudyType": "eqtl",
                    "chromosome": "3",
                },
                {
                    "leftLocusId": "2",
                    "rightLocusId": "3",
                    "rightStudyType": "eqtl",
                    "chromosome": "3",
                },
            ],
            "restrict_right_studies": ["EQTL_B"],
            "gwas_v_qtl_overlap_only": False,
        },
        {
            "id": "restrict_right_studies_gwas_right",
            "rows": [
                {
                    "locusId": "10",
                    "studyId": "GWAS_MAIN",
                    "studyType": "gwas",
                    "chromosome": "4",
                    "tagVariantId": "SNP1",
                },
                {
                    "locusId": "05",
                    "studyId": "GWAS_REF",
                    "studyType": "gwas",
                    "chromosome": "4",
                    "tagVariantId": "SNP1",
                },
                {
                    "locusId": "07",
                    "studyId": "GWAS_REF",
                    "studyType": "gwas",
                    "chromosome": "4",
                    "tagVariantId": "SNP2",
                },
            ],
            # Only overlaps where right.studyId in restrict list (GWAS_REF) and left not in it.
            # Two rows with GWAS_REF on SNP1 produce a single distinct pair (10,05).
            "expected": [
                {
                    "leftLocusId": "10",
                    "rightLocusId": "05",
                    "rightStudyType": "gwas",
                    "chromosome": "4",
                },
            ],
            "restrict_right_studies": ["GWAS_REF"],
            "gwas_v_qtl_overlap_only": False,
        },
        {
            "id": "no_overlaps",
            "rows": [
                {
                    "locusId": "1",
                    "studyId": "GWAS_A",
                    "studyType": "gwas",
                    "chromosome": "5",
                    "tagVariantId": "V1",
                },
                {
                    "locusId": "2",
                    "studyId": "EQTL_X",
                    "studyType": "eqtl",
                    "chromosome": "5",
                    "tagVariantId": "V2",
                },
            ],
            "expected": [],
            "restrict_right_studies": None,
            "gwas_v_qtl_overlap_only": False,
        },
    ],
    ids=lambda s: s["id"],
)
def test_overlapping_peaks_join_conditions(
    spark: SparkSession, scenario: dict[str, Any]
) -> None:
    """Comprehensive tests for _overlapping_peaks join-condition branches."""
    schema = t.StructType(
        [
            t.StructField("locusId", t.StringType()),
            t.StructField("studyId", t.StringType()),
            t.StructField("studyType", t.StringType()),
            t.StructField("chromosome", t.StringType()),
            t.StructField("tagVariantId", t.StringType()),
        ]
    )
    df = spark.createDataFrame(scenario["rows"], schema=schema)

    result = LocusOverlap._overlapping_peaks(
        df,
        restrict_right_studies=scenario["restrict_right_studies"],
        gwas_v_qtl_overlap_only=scenario["gwas_v_qtl_overlap_only"],
    ).select("leftLocusId", "rightLocusId", "rightStudyType", "chromosome")

    # Collect and compare as sets (ordering not guaranteed)
    observed = {tuple(r.asDict().items()) for r in result.collect()}
    expected_df = spark.createDataFrame(
        scenario["expected"],
        t.StructType(
            [
                t.StructField("leftLocusId", t.StringType()),
                t.StructField("rightLocusId", t.StringType()),
                t.StructField("rightStudyType", t.StringType()),
                t.StructField("chromosome", t.StringType()),
            ]
        ),
    )
    expected = {tuple(r.asDict().items()) for r in expected_df.collect()}

    assert observed == expected, (
        f"Scenario {scenario['id']} failed.\nObserved: {observed}\nExpected: {expected}"
    )

    # Additional invariant checks
    if scenario["gwas_v_qtl_overlap_only"]:
        # Ensure no GWAS-GWAS pairs
        assert result.filter(f.col("rightStudyType") == "gwas").count() == 0, (
            "GWAS-GWAS pair leaked in gwas_v_qtl_overlap_only mode."
        )

    if scenario["restrict_right_studies"] is not None:
        # Ensure all right studyIds are in restriction list
        right_ids = (
            df.alias("raw")
            .join(
                result,
                on=f.col("raw.locusId") == f.col("rightLocusId"),
                how="inner",
            )
            .select("studyId")
            .distinct()
            .rdd.flatMap(lambda r: r)
            .collect()
        )
        assert set(right_ids).issubset(set(scenario["restrict_right_studies"])), (
            "Right side includes studyId outside restrict_right_studies."
        )
