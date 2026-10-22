"""Tests of the credible set validation checks."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetConfidenceClasses,
    CredibleSetVariant,
)
from gentropy.dataset.study_index import StudyIndex, StudyQualityCheck
from gentropy.dataset.study_locus import StudyLocusQualityCheck
from gentropy.dataset.target_index import TargetIndex

QC = StudyLocusQualityCheck


def credible_sets(spark: SparkSession, rows: list[dict[str, Any]]) -> CredibleSet:
    """Credible sets from partial rows; the other columns take test defaults."""
    defaults = {
        "chromosome": "1",
        "locusStart": 1,
        "locusEnd": 1_000,
        "studyId": "s1",
        "studyType": "gwas",
        "leadVariantId": "1_100_A_T",
        "fineMappingMethod": "SuSiE",
        "qualityControls": [],
    }
    schema = CredibleSet.get_schema()
    data = [
        tuple({**defaults, **row}.get(c) for c in schema.fieldNames()) for row in rows
    ]
    return CredibleSet(spark.createDataFrame(data, schema), schema)


def variants(
    spark: SparkSession, rows: list[tuple[str, str, float]]
) -> CredibleSetVariant:
    """Credible set variants from (credibleSetId, variantId, posteriorProbability)."""
    schema = CredibleSetVariant.get_schema()
    defaults = {"pValueMantissa": 1.0, "pValueExponent": -10, "qualityControls": []}
    data = [
        tuple(
            {
                **defaults,
                "credibleSetId": cs,
                "variantId": v,
                "posteriorProbability": pip,
            }.get(c)
            for c in schema.fieldNames()
        )
        for cs, v, pip in rows
    ]
    return CredibleSetVariant(spark.createDataFrame(data, schema), schema)


def flags(cs: CredibleSet) -> dict[str, list[str]]:
    """Quality controls by credible set id."""
    return {r.id: r.qualityControls for r in cs.df.collect()}


def flagged(cs: CredibleSet, flag: QC) -> set[str]:
    """Ids of the credible sets carrying the flag."""
    return {i for i, q in flags(cs).items() if flag.value in q}


def test_qc_mhc_region(spark: SparkSession) -> None:
    """Only a lead inside the MHC is flagged."""
    cs = credible_sets(
        spark,
        [
            {"id": "in", "chromosome": "6", "leadVariantId": "6_30000000_A_T"},
            {"id": "out", "chromosome": "6", "leadVariantId": "6_50000000_A_T"},
            {"id": "other", "chromosome": "1", "leadVariantId": "1_30000000_A_T"},
        ],
    )
    assert flagged(cs.qc_mhc_region(), QC.IN_MHC) == {"in"}


def test_validate_chromosome_label(spark: SparkSession) -> None:
    """Chromosomes outside 1-22, X, Y, XY and MT are flagged."""
    cs = credible_sets(
        spark,
        [
            {"id": "a", "chromosome": "1"},
            {"id": "b", "chromosome": "X"},
            {"id": "c", "chromosome": "Un"},
        ],
    )
    assert flagged(cs.validate_chromosome_label(), QC.INVALID_CHROMOSOME) == {"c"}


def test_validate_study(spark: SparkSession) -> None:
    """Flagged and missing studies, and top hits of studies with summary statistics."""
    study_index = StudyIndex(
        spark.createDataFrame(
            [
                ("clean", "p", "gwas", []),
                ("flagged", "p", "gwas", ["Some study flag"]),
                (
                    "no_sumstats",
                    "p",
                    "gwas",
                    [StudyQualityCheck.SUMSTATS_NOT_AVAILABLE.value],
                ),
            ],
            "studyId string, projectId string, studyType string, qualityControls array<string>",
        ),
        StudyIndex.get_schema(),
    )
    cs = credible_sets(
        spark,
        [
            {"id": "a", "studyId": "clean"},
            {"id": "b", "studyId": "flagged"},
            {"id": "c", "studyId": "clean", "qualityControls": [QC.TOP_HIT.value]},
            {
                "id": "d",
                "studyId": "no_sumstats",
                "qualityControls": [QC.TOP_HIT.value],
            },
            {"id": "e", "studyId": "absent"},
        ],
    ).validate_study(study_index)
    assert flagged(cs, QC.FLAGGED_STUDY) == {"b", "d"}
    assert flagged(cs, QC.TOP_HIT_AND_SUMMARY_STATS) == {"c"}
    assert flagged(cs, QC.MISSING_STUDY) == {"e"}
    assert cs.df.count() == 5


def test_qc_redundant_top_hits_from_pics(spark: SparkSession) -> None:
    """A top hit is redundant only in a study with PICS credible sets from summary statistics."""
    top_hit = [QC.TOP_HIT.value]
    cs = credible_sets(
        spark,
        [
            {"id": "pics_sumstats", "studyId": "s1", "fineMappingMethod": "PICS"},
            {
                "id": "top_hit_1",
                "studyId": "s1",
                "fineMappingMethod": "PICS",
                "qualityControls": top_hit,
            },
            {
                "id": "top_hit_2",
                "studyId": "s2",
                "fineMappingMethod": "PICS",
                "qualityControls": top_hit,
            },
        ],
    )
    assert flagged(cs.qc_redundant_top_hits_from_pics(), QC.REDUNDANT_PICS_TOP_HIT) == {
        "top_hit_1"
    }


def test_qc_explained_by_susie(spark: SparkSession) -> None:
    """A non-SuSiE credible set with a variant inside a SuSiE region of its own study is flagged."""
    cs = credible_sets(
        spark,
        [
            {"id": "susie", "studyId": "s1", "locusStart": 100, "locusEnd": 200},
            {
                "id": "pics_in",
                "studyId": "s1",
                "fineMappingMethod": "PICS",
                "locusStart": 1,
                "locusEnd": 1_000,
            },
            {
                "id": "pics_out",
                "studyId": "s1",
                "fineMappingMethod": "PICS",
                "locusStart": 1,
                "locusEnd": 1_000,
            },
            {"id": "pics_other_study", "studyId": "s2", "fineMappingMethod": "PICS"},
        ],
    )
    members = variants(
        spark,
        [
            ("susie", "1_150_A_T", 1.0),
            ("pics_in", "1_50_A_T", 0.5),
            ("pics_in", "1_150_A_T", 0.5),
            ("pics_out", "1_500_A_T", 1.0),
            ("pics_other_study", "1_150_A_T", 1.0),
        ],
    )
    assert flagged(cs.qc_explained_by_susie(members), QC.EXPLAINED_BY_SUSIE) == {
        "pics_in"
    }


def test_qc_abnormal_pips(spark: SparkSession) -> None:
    """Credible sets whose posterior probabilities sum outside [0.95, 1.0001] are flagged."""
    cs = credible_sets(spark, [{"id": "ok"}, {"id": "low"}, {"id": "high"}])
    members = variants(
        spark,
        [
            ("ok", "1_1_A_T", 0.6),
            ("ok", "1_2_A_T", 0.37),
            ("low", "1_1_A_T", 0.5),
            ("high", "1_1_A_T", 0.7),
            ("high", "1_2_A_T", 0.5),
        ],
    )
    assert flagged(cs.qc_abnormal_pips(members), QC.ABNORMAL_PIPS) == {"low", "high"}


@pytest.mark.parametrize(
    ("method", "qc", "expected"),
    [
        ("SuSiE", [], CredibleSetConfidenceClasses.FINEMAPPED_IN_SAMPLE_LD),
        (
            "SuSiE-inf",
            [QC.OUT_OF_SAMPLE_LD.value],
            CredibleSetConfidenceClasses.FINEMAPPED_OUT_OF_SAMPLE_LD,
        ),
        (
            "PICS",
            [QC.OUT_OF_SAMPLE_LD.value],
            CredibleSetConfidenceClasses.PICSED_SUMMARY_STATS,
        ),
        ("PICS", [QC.TOP_HIT.value], CredibleSetConfidenceClasses.PICSED_TOP_HIT),
        ("other", [], CredibleSetConfidenceClasses.UNKNOWN),
    ],
)
def test_assign_confidence(
    spark: SparkSession,
    method: str,
    qc: list[str],
    expected: CredibleSetConfidenceClasses,
) -> None:
    """The confidence byte follows the fine-mapping method and the quality controls."""
    cs = credible_sets(
        spark, [{"id": "a", "fineMappingMethod": method, "qualityControls": qc}]
    )
    assert cs.assign_confidence().df.first().confidence == expected.value


def test_flag_trans_qtls(spark: SparkSession) -> None:
    """A QTL credible set is trans when all its variants are far from, or off the chromosome of, the gene."""
    study_index = StudyIndex(
        spark.createDataFrame(
            [
                ("gwas_study", "p", "gwas", None),
                ("qtl", "p", "eqtl", "g1"),
                ("qtl_no_gene", "p", "eqtl", None),
            ],
            "studyId string, projectId string, studyType string, geneId string",
        ),
        StudyIndex.get_schema(),
    )
    target_index = TargetIndex(
        spark.createDataFrame(
            [("g1", 1_000_000, ("1", 900_000, 1_100_000, 1))],
            "id string, tss long, genomicLocation struct<chromosome: string, start: long, end: long, strand: int>",
        ),
        TargetIndex.get_schema(),
    )
    cs = credible_sets(
        spark,
        [
            {"id": "cis", "studyId": "qtl"},
            {"id": "other_chromosome", "studyId": "qtl"},
            {"id": "far", "studyId": "qtl"},
            {"id": "mixed", "studyId": "qtl"},
            {"id": "gwas", "studyId": "gwas_study"},
            {"id": "no_gene", "studyId": "qtl_no_gene"},
        ],
    )
    members = variants(
        spark,
        [
            ("cis", "1_1000100_A_T", 1.0),
            ("other_chromosome", "2_1000000_A_T", 1.0),
            ("far", "1_9000000_A_T", 1.0),
            ("mixed", "1_9000000_A_T", 0.5),
            ("mixed", "1_1000100_A_T", 0.5),
            ("gwas", "2_1000000_A_T", 1.0),
            ("no_gene", "2_1000000_A_T", 1.0),
        ],
    )
    result = cs.flag_trans_qtls(members, study_index, target_index, 5_000_000)
    assert flagged(result, QC.TRANS_QTL) == {"other_chromosome", "far"}
    assert result.df.count() == 6


def test_validate_unique_id(spark: SparkSession) -> None:
    """Of two credible sets sharing an id, the one with fewer variants is kept."""
    cs = credible_sets(
        spark,
        [
            {"id": "dup", "fineMappingMethod": "PICS"},
            {"id": "dup", "fineMappingMethod": "SuSiE"},
            {"id": "single"},
        ],
    )
    members = variants(
        spark,
        [("dup", "1_1_A_T", 0.5), ("dup", "1_2_A_T", 0.5), ("single", "1_1_A_T", 1.0)],
    )
    rows = cs.validate_unique_id(members).df.collect()
    assert len(rows) == 3
    assert sorted(
        QC.DUPLICATED_STUDYLOCUS_ID.value in r.qualityControls for r in rows
    ) == [False, False, True]


class TestQcReplication:
    """Replication of lead variants across independent studies, as in the study locus validation."""

    CREDIBLE_SETS = [
        # Same lead variant and disease in two independent studies -> replicated:
        ("1", "v1", "s1"),
        ("2", "v1", "s2"),
        # s3 has the cohorts, publication and LD structure of s1, so it is not independent:
        ("3", "v2", "s1"),
        ("4", "v2", "s3"),
        # GWAS study without disease annotation -> not replicated:
        ("5", "v3", "s4"),
        # Same lead variant and gene in two molQTL studies -> replicated:
        ("6", "v4", "s5"),
        ("7", "v4", "s6"),
        # Only molQTL study of this gene at this variant -> not replicated:
        ("8", "v4", "s7"),
        # molQTL study without measured gene -> not replicated:
        ("9", "v5", "s8"),
        # The same two diseases in a different order -> replicated:
        ("10", "v6", "s9"),
        ("11", "v6", "s10"),
        # One disease out of a longer list -> not replicated:
        ("12", "v7", "s11"),
        ("13", "v7", "s1"),
        # Measurements only: one shared measurement is enough -> replicated:
        ("14", "v8", "s12"),
        ("15", "v8", "s13"),
        # Measurement and disease: the measurement is set aside, the disease matches -> replicated:
        ("16", "v9", "s14"),
        ("17", "v9", "s15"),
        # Measurement and disease: same measurement, different disease -> not replicated:
        ("18", "v10", "s14"),
        ("19", "v10", "s16"),
    ]
    STUDIES = [
        ("s1", "p1", "gwas", ["d1"], None, ["c1"], "pm1", [("nfe", 1.0)]),
        ("s2", "p1", "gwas", ["d1"], None, ["c2"], "pm2", [("fin", 1.0)]),
        ("s3", "p1", "gwas", ["d1"], None, ["c1"], "pm1", [("nfe", 1.0)]),
        ("s4", "p1", "gwas", None, None, ["c3"], "pm3", [("nfe", 1.0)]),
        ("s5", "p2", "eqtl", None, "g1", None, None, None),
        ("s6", "p2", "eqtl", None, "g1", None, None, None),
        ("s7", "p2", "eqtl", None, "g2", None, None, None),
        ("s8", "p2", "eqtl", None, None, None, None, None),
        ("s9", "p1", "gwas", ["d1", "d2"], None, ["c4"], "pm4", [("nfe", 1.0)]),
        ("s10", "p1", "gwas", ["d2", "d1"], None, ["c5"], "pm5", [("nfe", 1.0)]),
        ("s11", "p1", "gwas", ["d1", "d3"], None, ["c6"], "pm6", [("nfe", 1.0)]),
        ("s12", "p1", "gwas", ["m1", "m2"], None, ["c7"], "pm7", [("nfe", 1.0)]),
        ("s13", "p1", "gwas", ["m1", "m3"], None, ["c8"], "pm8", [("nfe", 1.0)]),
        ("s14", "p1", "gwas", ["m1", "d1"], None, ["c9"], "pm9", [("nfe", 1.0)]),
        ("s15", "p1", "gwas", ["m2", "d1"], None, ["c10"], "pm10", [("nfe", 1.0)]),
        ("s16", "p1", "gwas", ["m1", "d2"], None, ["c11"], "pm11", [("nfe", 1.0)]),
    ]
    DISEASES = [
        ("m1", ["EFO_0001444"]),
        ("m2", ["EFO_0001444"]),
        ("m3", ["EFO_0001444"]),
        ("d1", ["MONDO_0045024"]),
        ("d2", ["MONDO_0045024"]),
    ]

    @pytest.fixture(autouse=True)
    def _setup(self: TestQcReplication, spark: SparkSession) -> None:
        """Credible sets, study index and disease index of the cases above."""
        self.credible_sets = credible_sets(
            spark,
            [
                {"id": i, "leadVariantId": v, "studyId": s}
                for i, v, s in self.CREDIBLE_SETS
            ],
        )
        study_index = StudyIndex(
            spark.createDataFrame(
                self.STUDIES,
                "studyId string, projectId string, studyType string, diseaseIds array<string>, "
                "geneId string, cohorts array<string>, pubmedId string, "
                "ldPopulationStructure array<struct<ldPopulation: string, relativeSampleSize: double>>",
            ),
            StudyIndex.get_schema(),
        )
        disease_index = spark.createDataFrame(
            self.DISEASES, "id string, therapeuticAreas array<string>"
        )
        self.validated = self.credible_sets.qc_replication(study_index, disease_index)

    def test_no_data_loss(self: TestQcReplication) -> None:
        """Every credible set is kept."""
        assert self.validated.df.count() == len(self.CREDIBLE_SETS)

    def test_correctness(self: TestQcReplication) -> None:
        """Only the credible sets with independent replication are flagged."""
        assert flagged(self.validated, QC.REPLICATED) == {
            "1", "2", "6", "7", "10", "11", "14", "15", "16", "17",
        }  # fmt: skip


def test_flag_helpers_keep_null_quality_controls_empty(spark: SparkSession) -> None:
    """A check on a credible set with no flag leaves its quality controls an empty array."""
    cs = credible_sets(spark, [{"id": "a"}]).validate_chromosome_label()
    assert cs.df.select(f.size("qualityControls")).first()[0] == 0
    assert cs.df.schema["qualityControls"].dataType == t.ArrayType(
        t.StringType(), False
    )
