"""Tests for the predicted pleiotropy prior locus-to-gene features."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_features.pleiotropy_prior import (
    ApproxPopsFeature,
    ApproxPopsNeighbourhoodFeature,
    PleiotropyPriorInputs,
    PredictedPleiotropyPriorFeature,
    PredictedPleiotropyPriorNeighbourhoodFeature,
    gene_covariates,
    nearest_gene_disease_counts,
)
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.l2g.feature_factory import FeatureFactory, L2GFeatureInputLoader

CONSEQUENCE = StructType(
    [
        StructField("distanceFromFootprint", LongType(), True),
        StructField("distanceFromTss", LongType(), True),
        StructField("targetId", StringType(), True),
        StructField("isEnsemblCanonical", BooleanType(), True),
        StructField("biotype", StringType(), True),
    ]
)
VARIANT_SCHEMA = StructType(
    [
        StructField("variantId", StringType(), True),
        StructField("chromosome", StringType(), True),
        StructField("position", IntegerType(), True),
        StructField("referenceAllele", StringType(), True),
        StructField("alternateAllele", StringType(), True),
        StructField("transcriptConsequences", ArrayType(CONSEQUENCE), True),
    ]
)


def _target_index(spark: SparkSession, genes: list[tuple[Any, ...]]) -> TargetIndex:
    """Target index from (id, biotype, chromosome, start, end, tss) tuples."""
    return TargetIndex(
        _df=spark.createDataFrame(
            genes,
            "id string, biotype string, chromosome string, start long, end long, tss long",
        ).select(
            "id",
            "biotype",
            f.struct("chromosome", "start", "end").alias("genomicLocation"),
            "tss",
        ),
        _schema=TargetIndex.get_schema(),
    )


def _credible_sets(
    spark: SparkSession, rows: list[tuple[str, str, str, list[str]]]
) -> StudyLocus:
    """Credible sets from (studyLocusId, lead variantId, studyId, locus variantIds) tuples."""
    return StudyLocus(
        _df=spark.createDataFrame(
            [
                (sl, lead, study, [(v, 0.5) for v in locus])
                for sl, lead, study, locus in rows
            ],
            "studyLocusId string, variantId string, studyId string, "
            "locus array<struct<variantId: string, posteriorProbability: double>>",
        )
        .withColumn("chromosome", f.lit("1"))
        .withColumn("position", f.lit(2000)),
        _schema=StudyLocus.get_schema(),
    )


def _variants(
    spark: SparkSession, rows: dict[str, list[tuple[str, str, int]]]
) -> VariantIndex:
    """Variant index from {variantId: [(geneId, biotype, distanceFromFootprint)]}."""
    return VariantIndex(
        _df=spark.createDataFrame(
            [
                (
                    variant,
                    "1",
                    2000,
                    "A",
                    "T",
                    [(d, d, gene, True, biotype) for gene, biotype, d in genes],
                )
                for variant, genes in rows.items()
            ],
            VARIANT_SCHEMA,
        ),
        _schema=VariantIndex.get_schema(),
    )


def _scores(feature: L2GFeature, study_locus_id: str) -> dict[str, float]:
    """Feature values of one credible set, by gene."""
    return {
        row["geneId"]: float(row["featureValue"])
        for row in feature.df.filter(f.col("studyLocusId") == study_locus_id).collect()
    }


class TestPleiotropyPriorTarget:
    """Test the target and covariates the prior is fitted on.

    Three protein-coding genes and one lncRNA on chromosome 1. Variant v1 is nearest to geneA
    once the lncRNA is set aside, and variant v2 is as close to geneA as to geneC.
    """

    @pytest.fixture()
    def target_index(
        self: TestPleiotropyPriorTarget, spark: SparkSession
    ) -> TargetIndex:
        """Genes with positions, all within 500 kb of each other."""
        return _target_index(
            spark,
            [
                ("geneA", "protein_coding", "1", 1_000_000, 1_009_999, 1_000_000),
                ("geneB", "protein_coding", "1", 1_100_000, 1_100_999, 1_100_000),
                ("geneC", "protein_coding", "1", 1_004_000, 1_004_999, 1_004_000),
                ("geneL", "lncRNA", "1", 1_001_000, 1_001_999, 1_001_000),
            ],
        )

    @pytest.fixture()
    def variant_index(
        self: TestPleiotropyPriorTarget, spark: SparkSession
    ) -> VariantIndex:
        """Two lead variants with their distances to the genes."""
        return VariantIndex(
            _df=spark.createDataFrame(
                [
                    (
                        "v1",
                        "1",
                        1_001_000,
                        "A",
                        "T",
                        [
                            (0, 1_000, "geneA", True, "protein_coding"),
                            (0, 99_000, "geneB", True, "protein_coding"),
                            (0, 10, "geneL", True, "lncRNA"),
                        ],
                    ),
                    (
                        "v2",
                        "1",
                        1_002_000,
                        "A",
                        "T",
                        [
                            (0, 2_000, "geneA", True, "protein_coding"),
                            (0, 2_000, "geneC", True, "protein_coding"),
                            (0, 98_000, "geneB", True, "protein_coding"),
                        ],
                    ),
                ],
                VARIANT_SCHEMA,
            ),
            _schema=VariantIndex.get_schema(),
        )

    @pytest.fixture()
    def study_index(self: TestPleiotropyPriorTarget, spark: SparkSession) -> StudyIndex:
        """Three GWAS studies and a molecular QTL study."""
        return StudyIndex(
            _df=spark.createDataFrame(
                [
                    ("s1", "gwas", "p", ["d1", "d2"]),
                    ("s2", "gwas", "p", ["d1"]),
                    ("s3", "gwas", "p", ["d3"]),
                    ("s4", "eqtl", "p", ["d4"]),
                ],
                "studyId string, studyType string, projectId string, diseaseIds array<string>",
            ),
            _schema=StudyIndex.get_schema(),
        )

    @pytest.fixture()
    def study_locus(self: TestPleiotropyPriorTarget, spark: SparkSession) -> StudyLocus:
        """One credible set per study."""
        return StudyLocus(
            _df=spark.createDataFrame(
                [
                    ("cs1", "s1", "v1", "gwas"),
                    ("cs2", "s2", "v1", "gwas"),
                    ("cs3", "s3", "v2", "gwas"),
                    ("cs4", "s4", "v2", "eqtl"),
                ],
                "studyLocusId string, studyId string, variantId string, studyType string",
            ).withColumn("chromosome", f.lit("1")),
            _schema=StudyLocus.get_schema(),
        )

    def test_nearest_gene_disease_counts(
        self: TestPleiotropyPriorTarget,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> None:
        """Diseases count once per gene, ties keep both genes and QTL studies do not count."""
        observed = {
            row["geneId"]: row["targetCount"]
            for row in nearest_gene_disease_counts(
                study_locus, study_index, variant_index, target_index
            ).collect()
        }
        # geneA: d1 and d2 through cs1, d1 again through cs2, d3 through the tie at cs3.
        # geneC: d3 through the tie. geneB is never nearest; d4 comes from an eQTL study.
        assert observed == {"geneA": 3, "geneC": 1}

    def test_gene_covariates(
        self: TestPleiotropyPriorTarget, target_index: TargetIndex
    ) -> None:
        """Gene length and the number of protein-coding TSSs around the gene, logged."""
        observed = {
            row["geneId"]: (row["logGeneLength"], row["logNeighbouringGenes"])
            for row in gene_covariates(target_index).collect()
        }
        assert set(observed) == {"geneA", "geneB", "geneC"}
        # The gene's own TSS counts among the TSSs in its window.
        assert observed["geneA"] == pytest.approx((math.log(10_000), math.log(4)))
        assert observed["geneB"] == pytest.approx((math.log(1_000), math.log(4)))
        nearby = gene_covariates(target_index, genomic_window=50_000)
        assert {
            row["geneId"]: row["logNeighbouringGenes"] for row in nearby.collect()
        } == pytest.approx(
            {"geneA": math.log(3), "geneB": math.log(2), "geneC": math.log(3)}
        )


class TestPleiotropyPriorFeatureLogic:
    """Test how the gene prior becomes features, with the prior fixed in advance.

    The scores cover gene1 (0.6), gene2 (-0.2), gene3 (-0.4), gene5 (2.0) and gene7 (0.1).
    The lead variant of sl1 reaches genes 1 to 6: gene4 is protein coding but has no score,
    gene5 has a non-coding biotype, and gene6 is more than 500 kb away. gene7 is reached only
    through the second variant of sl1. sl2 only reaches gene3.
    """

    @pytest.fixture()
    def dependencies(
        self: TestPleiotropyPriorFeatureLogic, spark: SparkSession
    ) -> dict[str, Any]:
        """Fitted scores, two credible sets and the genes around their variants."""
        inputs = PleiotropyPriorInputs(*[spark.createDataFrame([], "x int")] * 4)
        inputs._scores = spark.createDataFrame(
            [
                ("gene1", 0.6),
                ("gene2", -0.2),
                ("gene3", -0.4),
                ("gene5", 2.0),
                ("gene7", 0.1),
            ],
            "geneId string, predictedPleiotropyPrior double",
        )
        return {
            "pleiotropy_prior_inputs": inputs,
            "study_locus": _credible_sets(
                spark,
                [
                    ("sl1", "var1", "study1", ["var1", "var3"]),
                    ("sl2", "var2", "study1", ["var2"]),
                ],
            ),
            "study_index": None,
            "variant_index": _variants(
                spark,
                {
                    "var1": [
                        ("gene1", "protein_coding", 0),
                        ("gene2", "protein_coding", 1000),
                        ("gene3", "protein_coding", 2000),
                        ("gene4", "protein_coding", 500),
                        ("gene5", "lncRNA", 500),
                        ("gene6", "protein_coding", 600_000),
                    ],
                    "var2": [("gene3", "protein_coding", 0)],
                    "var3": [("gene7", "protein_coding", 400_000)],
                },
            ),
            "target_index": None,
        }

    def test_local_feature_is_the_gene_score(
        self: TestPleiotropyPriorFeatureLogic, dependencies: dict[str, Any]
    ) -> None:
        """One row per credible set and scored protein-coding gene in the window."""
        feature = PredictedPleiotropyPriorFeature.compute(
            study_loci_to_annotate=dependencies["study_locus"],
            feature_dependency=dependencies,
        )
        assert _scores(feature, "sl1") == {
            "gene1": pytest.approx(0.6),
            "gene2": pytest.approx(-0.2),
            "gene3": pytest.approx(-0.4),
            "gene7": pytest.approx(0.1),
        }
        assert _scores(feature, "sl2") == {"gene3": pytest.approx(-0.4)}
        assert feature.df.count() == 5

    def test_neighbourhood_is_scaled_between_the_locus_minimum_and_maximum(
        self: TestPleiotropyPriorFeatureLogic, dependencies: dict[str, Any]
    ) -> None:
        """The best gene at a locus gets 1, the worst 0, and a lone gene 1."""
        feature = PredictedPleiotropyPriorNeighbourhoodFeature.compute(
            study_loci_to_annotate=dependencies["study_locus"],
            feature_dependency=dependencies,
        )
        assert _scores(feature, "sl1") == {
            "gene1": pytest.approx(1.0),
            "gene2": pytest.approx(0.2),
            "gene3": pytest.approx(0.0),
            "gene7": pytest.approx(0.5),
        }
        assert _scores(feature, "sl2") == {"gene3": pytest.approx(1.0)}
        assert {
            row["featureName"] for row in feature.df.select("featureName").collect()
        } == {"predictedPleiotropyPriorNeighbourhood"}


class TestApproxPopsFeatureLogic:
    """Test how per-disease scores become features, with the scores fixed in advance.

    sl1 belongs to study1, mapped to d1 and d2; sl2 to study2, mapped to d9, which has no
    scores. gene1 scores 0.5 for d1 and 1.5 for d2, gene2 -1.0 for d1, and gene3 2.0 only for
    d3, a disease of neither study.
    """

    @pytest.fixture()
    def dependencies(
        self: TestApproxPopsFeatureLogic, spark: SparkSession
    ) -> dict[str, Any]:
        """Fitted scores, two credible sets, their studies and the genes around their variants."""
        inputs = PleiotropyPriorInputs(*[spark.createDataFrame([], "x int")] * 4)
        inputs._approx_pops = spark.createDataFrame(
            [
                ("gene1", "d1", 0.5),
                ("gene1", "d2", 1.5),
                ("gene2", "d1", -1.0),
                ("gene3", "d3", 2.0),
            ],
            "geneId string, diseaseId string, approxPops double",
        )
        study_locus = _credible_sets(
            spark,
            [
                ("sl1", "var1", "study1", ["var1"]),
                ("sl2", "var2", "study2", ["var2"]),
            ],
        )
        study_index = StudyIndex(
            _df=spark.createDataFrame(
                [
                    ("study1", "gwas", "p", ["d1", "d2"]),
                    ("study2", "gwas", "p", ["d9"]),
                ],
                "studyId string, studyType string, projectId string, "
                "diseaseIds array<string>",
            ),
            _schema=StudyIndex.get_schema(),
        )
        variant_index = _variants(
            spark,
            {
                "var1": [
                    ("gene1", "protein_coding", 0),
                    ("gene2", "protein_coding", 1000),
                    ("gene3", "protein_coding", 2000),
                ],
                "var2": [("gene1", "protein_coding", 0)],
            },
        )
        return {
            "pleiotropy_prior_inputs": inputs,
            "study_locus": study_locus,
            "study_index": study_index,
            "variant_index": variant_index,
            "target_index": None,
        }

    def test_local_feature_is_the_highest_score_over_the_study_diseases(
        self: TestApproxPopsFeatureLogic, dependencies: dict[str, Any]
    ) -> None:
        """A gene takes its best score over the study's diseases; other diseases do not count."""
        feature = ApproxPopsFeature.compute(
            study_loci_to_annotate=dependencies["study_locus"],
            feature_dependency=dependencies,
        )
        assert _scores(feature, "sl1") == {
            "gene1": pytest.approx(1.5),
            "gene2": pytest.approx(-1.0),
        }
        assert _scores(feature, "sl2") == {}

    def test_neighbourhood_is_scaled_within_the_locus(
        self: TestApproxPopsFeatureLogic, dependencies: dict[str, Any]
    ) -> None:
        """The best gene at a locus gets 1 and the worst 0."""
        feature = ApproxPopsNeighbourhoodFeature.compute(
            study_loci_to_annotate=dependencies["study_locus"],
            feature_dependency=dependencies,
        )
        assert _scores(feature, "sl1") == {
            "gene1": pytest.approx(1.0),
            "gene2": pytest.approx(0.0),
        }


def test_feature_block() -> None:
    """Expression level and specificity are blocks of their own; other features go by source."""
    assert PleiotropyPriorInputs.feature_block("gtex:level:UBERON_1|") == "gtex:level"
    assert (
        PleiotropyPriorInputs.feature_block("pride:specificity:UBERON_1|")
        == "pride:specificity"
    )
    assert PleiotropyPriorInputs.feature_block("go:GO:0001") == "go"
    assert PleiotropyPriorInputs.feature_block("constraint:lof:oe") == "constraint"
    assert (
        PleiotropyPriorInputs.feature_block("essentiality:isEssential")
        == "essentiality"
    )


def _annotated_target_index(
    spark: SparkSession, rows: list[dict[str, Any]]
) -> TargetIndex:
    """Target index from dictionaries with any of its fields."""
    return TargetIndex(
        _df=spark.createDataFrame(rows, TargetIndex.get_schema()),
        _schema=TargetIndex.get_schema(),
    )


def _gene(
    gene_id: str, chromosome: str, tss: int, **annotations: Any
) -> dict[str, Any]:
    """Protein-coding target index row."""
    return {
        "id": gene_id,
        "biotype": "protein_coding",
        "genomicLocation": {"chromosome": chromosome, "start": tss, "end": tss + 999},
        "tss": tss,
        **annotations,
    }


INTERACTION_SCHEMA = (
    "sourceDatabase string, targetA string, targetB string, scoring double"
)
MOUSE_SCHEMA = "targetFromSourceId string, modelPhenotypeId string"
EXPRESSION_SCHEMA = (
    "targetId string, datasourceId string, tissueBiosampleId string, "
    "celltypeBiosampleId string, median double"
)
ESSENTIALITY_SCHEMA = "targetId string, isEssential boolean"


class TestPleiotropyPriorGeneFeatures:
    """Test the gene features built from the Open Targets gene data.

    Twelve protein-coding genes g00 to g11 and one lncRNA. Every block holds one term that is
    kept and, where it matters, one that falls under the minimum size once only protein-coding
    genes are counted.
    """

    @pytest.fixture()
    def built(
        self: TestPleiotropyPriorGeneFeatures, spark: SparkSession
    ) -> tuple[dict[tuple[str, str], float], set[tuple[str, str]]]:
        """Features as (gene, feature) -> value and coverage as (gene, source) pairs."""
        genes = [f"g{i:02d}" for i in range(12)]
        rows = []
        for i, gene in enumerate(genes):
            go = [{"id": "GO:big"}] if i < 10 else []
            go += [{"id": "GO:small"}] if i < 9 else []
            constraint = []
            if i < 2:
                constraint = [{"constraintType": "lof", "oe": 0.2 + 0.2 * i}]
            rows.append(
                _gene(
                    gene,
                    "1",
                    1_000_000 * (i + 1),
                    go=go,
                    pathways=[{"pathwayId": "R-1"}],
                    constraint=constraint,
                )
            )
        rows.append(
            {
                "id": "lnc",
                "biotype": "lncRNA",
                "genomicLocation": {"chromosome": "1"},
                "tss": 1,
                "go": [{"id": "GO:small"}],
            }
        )
        target_index = _annotated_target_index(spark, rows)
        interactions = spark.createDataFrame(
            [("string", "g00", gene, 0.8) for gene in genes[1:11]]
            + [
                ("string", "g11", "g01", 0.6),
                ("intact", "g11", "g02", 0.5),
                ("reactome", "g11", "g03", 1.0),
                ("string", "g04", "g04", 0.9),
            ],
            INTERACTION_SCHEMA,
        )
        mouse_phenotype = spark.createDataFrame(
            [(gene, "MP:1") for gene in genes[:10]] + [("ENSG_x", "MP:1")],
            MOUSE_SCHEMA,
        )
        baseline_expression = spark.createDataFrame(
            [
                ("g00", "gtex", "T1", None, 1.0),
                ("g00", "gtex", "T1", None, 3.0),
                ("g00", "gtex", "T2", None, -5.0),
                ("g00", "other_source", "T1", None, 9.0),
            ],
            EXPRESSION_SCHEMA,
        )
        target_essentiality = spark.createDataFrame(
            [("g00", True), ("g01", False)], ESSENTIALITY_SCHEMA
        )
        inputs = PleiotropyPriorInputs(
            interactions, mouse_phenotype, baseline_expression, target_essentiality
        )
        features, coverage = inputs.gene_features(
            gene_covariates(target_index), target_index
        )
        return (
            {
                (row["geneId"], row["featureId"]): row["value"]
                for row in features.collect()
            },
            {(row["geneId"], row["source"]) for row in coverage.collect()},
        )

    def test_binary_blocks_keep_terms_within_the_size_window(
        self: TestPleiotropyPriorGeneFeatures,
        built: tuple[dict[tuple[str, str], float], set[tuple[str, str]]],
    ) -> None:
        """Terms with 10 to 2,000 protein-coding genes are kept, smaller ones dropped."""
        features, _ = built
        binary = {
            feature_id
            for _, feature_id in features
            if feature_id.split(":")[0] in {"go", "reactome", "ppi", "mouse_phenotype"}
        }
        # GO:small has ten members, but only nine protein-coding ones. Only g00 has ten
        # high-confidence neighbours.
        assert binary == {
            "go:GO:big",
            "reactome:R-1",
            "ppi:g00",
            "mouse_phenotype:MP:1",
        }
        assert features[("g05", "ppi:g00")] == 1.0
        assert ("g11", "ppi:g00") not in features

    def test_interaction_thresholds(
        self: TestPleiotropyPriorGeneFeatures,
        built: tuple[dict[tuple[str, str], float], set[tuple[str, str]]],
    ) -> None:
        """Only STRING >= 0.7 and IntAct >= 0.45 edges between distinct genes count."""
        _, coverage = built
        # g11's IntAct edge passes; its weak STRING and its Reactome edge do not matter.
        assert ("g11", "ppi") in coverage
        assert {gene for gene, source in coverage if source == "ppi"} == {
            f"g{i:02d}" for i in range(12)
        }

    def test_expression_level_and_specificity(
        self: TestPleiotropyPriorGeneFeatures,
        built: tuple[dict[tuple[str, str], float], set[tuple[str, str]]],
    ) -> None:
        """Repeats are averaged, negative medians floor at 0, specificity is a z-score."""
        features, coverage = built
        assert features[("g00", "gtex:level:T1|")] == pytest.approx(
            (math.log(2) + math.log(4)) / 2
        )
        assert features[("g00", "gtex:level:T2|")] == pytest.approx(0.0)
        assert features[("g00", "gtex:specificity:T1|")] == pytest.approx(1.0)
        assert features[("g00", "gtex:specificity:T2|")] == pytest.approx(-1.0)
        assert not any(
            feature_id.startswith("other_source") for _, feature_id in features
        )
        assert ("g00", "expression:gtex") in coverage

    def test_constraint_is_imputed_with_the_mean(
        self: TestPleiotropyPriorGeneFeatures,
        built: tuple[dict[tuple[str, str], float], set[tuple[str, str]]],
    ) -> None:
        """Every gene gets every constraint feature, missing values at the mean."""
        features, coverage = built
        assert features[("g00", "constraint:lof:oe")] == pytest.approx(0.2)
        assert features[("g05", "constraint:lof:oe")] == pytest.approx(0.3)
        assert sum(feature == "constraint:lof:oe" for _, feature in features) == 12
        assert {gene for gene, source in coverage if source == "constraint"} == {
            "g00",
            "g01",
        }

    def test_essentiality(
        self: TestPleiotropyPriorGeneFeatures,
        built: tuple[dict[tuple[str, str], float], set[tuple[str, str]]],
    ) -> None:
        """Essentiality is 0/1 for the genes the dataset covers."""
        features, coverage = built
        assert features[("g00", "essentiality:isEssential")] == 1.0
        assert features[("g01", "essentiality:isEssential")] == 0.0
        assert {gene for gene, source in coverage if source == "essentiality"} == {
            "g00",
            "g01",
        }


class TestPleiotropyPriorEndToEnd:
    """Fit the prior from toy Open Targets gene data through the feature factory.

    Twelve genes 200 kb apart on each of chromosomes 1, 2 and 6, those on chromosome 6 inside
    the MHC. Every gene is the nearest gene of one credible set, whose study carries zero to
    three diseases.
    """

    CHROMOSOMES = {"1": 1_000_000, "2": 1_000_000, "6": 29_000_000}
    GENES_PER_CHROMOSOME = 12

    @pytest.fixture()
    def release(
        self: TestPleiotropyPriorEndToEnd, spark: SparkSession
    ) -> dict[str, Any]:
        """Datasets of the toy release."""
        rng = np.random.default_rng(3)
        genes = [
            (f"chr{chromosome}_gene{i}", chromosome, first_tss + 200_000 * i)
            for chromosome, first_tss in self.CHROMOSOMES.items()
            for i in range(self.GENES_PER_CHROMOSOME)
        ]
        gene_ids = [gene for gene, _, _ in genes]

        def members(n: int) -> list[str]:
            return [str(gene) for gene in rng.choice(gene_ids, size=n, replace=False)]

        go_terms = {f"GO:{t}": set(members(15)) for t in range(6)}
        pathways = {f"R-{t}": set(members(12)) for t in range(3)}
        target_rows, variants, credible_sets, studies = [], [], [], []
        for i, (gene, chromosome, tss) in enumerate(genes):
            target_rows.append(
                _gene(
                    gene,
                    chromosome,
                    tss,
                    go=[{"id": t} for t, g in go_terms.items() if gene in g],
                    pathways=[
                        {"pathwayId": t} for t, g in pathways.items() if gene in g
                    ],
                    constraint=(
                        [
                            {
                                "constraintType": "lof",
                                "oe": float(rng.uniform()),
                                "oeUpper": float(rng.uniform()),
                                "score": float(rng.normal()),
                            },
                            {
                                "constraintType": "mis",
                                "oe": float(rng.uniform()),
                                "score": float(rng.normal()),
                            },
                            {"constraintType": "syn", "score": float(rng.normal())},
                        ]
                        if i % 3
                        else []
                    ),
                )
            )
            neighbour = genes[i + 1][0] if i + 1 < len(genes) else genes[i - 1][0]
            variants.append(
                (
                    f"v{i}",
                    chromosome,
                    tss + 100,
                    "A",
                    "T",
                    [
                        (0, 100, gene, True, "protein_coding"),
                        (0, 199_900, neighbour, True, "protein_coding"),
                    ],
                )
            )
            n_diseases = int(rng.integers(0, 4))
            studies.append((f"s{i}", "gwas", "p", [f"d{j}" for j in range(n_diseases)]))
            credible_sets.append(
                (f"cs{i}", f"s{i}", f"v{i}", chromosome, tss + 100, "gwas")
            )

        return {
            "pleiotropy_prior_inputs": PleiotropyPriorInputs(
                spark.createDataFrame(
                    [("string", gene_ids[0], gene, 0.9) for gene in members(12)],
                    INTERACTION_SCHEMA,
                ),
                spark.createDataFrame(
                    [(gene, f"MP:{t}") for t in range(3) for gene in members(11)],
                    MOUSE_SCHEMA,
                ),
                spark.createDataFrame(
                    [
                        (gene, "gtex", f"T{t}", None, float(rng.gamma(2.0)))
                        for gene in gene_ids
                        for t in range(3)
                    ],
                    EXPRESSION_SCHEMA,
                ),
                spark.createDataFrame(
                    [(gene, bool(rng.integers(0, 2))) for gene in gene_ids[::2]],
                    ESSENTIALITY_SCHEMA,
                ),
                lambda_grid=[0.1, 1.0, 10.0],
            ),
            "study_locus": StudyLocus(
                _df=spark.createDataFrame(
                    credible_sets,
                    "studyLocusId string, studyId string, variantId string, "
                    "chromosome string, position integer, studyType string",
                ).withColumn(
                    "locus",
                    f.array(
                        f.struct(
                            f.col("variantId").alias("variantId"),
                            f.lit(1.0).alias("posteriorProbability"),
                        )
                    ),
                ),
                _schema=StudyLocus.get_schema(),
            ),
            "study_index": StudyIndex(
                _df=spark.createDataFrame(
                    studies,
                    "studyId string, studyType string, projectId string, "
                    "diseaseIds array<string>",
                ),
                _schema=StudyIndex.get_schema(),
            ),
            "variant_index": VariantIndex(
                _df=spark.createDataFrame(variants, VARIANT_SCHEMA),
                _schema=VariantIndex.get_schema(),
            ),
            "target_index": _annotated_target_index(spark, target_rows),
        }

    def test_feature_factory_fits_once_and_builds_both_features(
        self: TestPleiotropyPriorEndToEnd,
        release: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both features come from a single fit and cover every gene in each window."""
        fits = []
        fit = PleiotropyPriorInputs._fit

        def counting_fit(self: PleiotropyPriorInputs, *args: Any) -> Any:
            fits.append(1)
            return fit(self, *args)

        monkeypatch.setattr(PleiotropyPriorInputs, "_fit", counting_fit)

        local, neighbourhood = FeatureFactory(
            release["study_locus"],
            ["predictedPleiotropyPrior", "predictedPleiotropyPriorNeighbourhood"],
        ).generate_features(L2GFeatureInputLoader(**release))

        assert len(fits) == 1
        local_rows = local.df.toPandas()
        neighbourhood_rows = neighbourhood.df.toPandas()
        # The variant index links each lead variant to two genes, its own and a neighbour.
        assert len(local_rows) == len(neighbourhood_rows)
        assert (local_rows.groupby("studyLocusId").size() == 2).all()
        assert local_rows["geneId"].nunique() == 36
        assert np.isfinite(local_rows["featureValue"]).all()
        assert (local_rows["featureValue"] != 0).any()
        assert neighbourhood_rows["featureValue"].between(0.0, 1.0).all()
        assert (
            neighbourhood_rows.groupby("studyLocusId")["featureValue"].max() == 1.0
        ).all()

    def test_feature_factory_builds_approx_pops(
        self: TestPleiotropyPriorEndToEnd,
        release: dict[str, Any],
    ) -> None:
        """Every non-MHC gene is scored for each kept disease, and both features are built.

        The study of each credible set is remapped to one disease, dGO, when its nearest gene is
        annotated with GO:0, so that the gene features predict that disease.
        """
        in_go0 = {
            row["id"]
            for row in release["target_index"].df.collect()
            if any(term["id"] == "GO:0" for term in row["go"] or [])
        }
        gene_of_variant = {
            row["variantId"]: row["transcriptConsequences"][0]["targetId"]
            for row in release["variant_index"].df.collect()
        }
        go0_studies = [
            row["studyId"]
            for row in release["study_locus"].df.collect()
            if gene_of_variant[row["variantId"]] in in_go0
        ]
        release["study_index"] = StudyIndex(
            _df=release["study_index"].df.withColumn(
                "diseaseIds",
                f.when(
                    f.col("studyId").isin(go0_studies), f.array(f.lit("dGO"))
                ).otherwise(f.array().cast("array<string>")),
            ),
            _schema=StudyIndex.get_schema(),
        )
        inputs = release["pleiotropy_prior_inputs"]
        inputs.approx_pops_min_genes = 3
        inputs.approx_pops_components = 20
        local, neighbourhood = FeatureFactory(
            release["study_locus"], ["approxPops", "approxPopsNeighbourhood"]
        ).generate_features(L2GFeatureInputLoader(**release))

        scores = inputs.approx_pops_scores(
            release["study_locus"],
            release["study_index"],
            release["variant_index"],
            release["target_index"],
        ).toPandas()
        # Genes on chromosome 6 sit in the MHC and get no score.
        assert set(scores["diseaseId"]) == {"dGO"}
        assert not scores["geneId"].str.startswith("chr6_").any()
        assert (scores.groupby("diseaseId").size() == 24).all()
        local_rows = local.df.toPandas()
        neighbourhood_rows = neighbourhood.df.toPandas()
        assert len(local_rows) == len(neighbourhood_rows)
        assert np.isfinite(local_rows["featureValue"]).all()
        assert neighbourhood_rows["featureValue"].between(0.0, 1.0).all()
