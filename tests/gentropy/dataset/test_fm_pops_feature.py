"""Tests for the fmPops locus-to-gene features."""

from __future__ import annotations

import math
from pathlib import Path
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

from gentropy.dataset.l2g_features.fm_pops import (
    FmPopsFeature,
    FmPopsNeighbourhoodFeature,
    PopsGeneFeatures,
    gene_covariates,
    nearest_gene_disease_counts,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
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


def _scores(feature: L2GFeature, study_locus_id: str) -> dict[str, float]:
    """Feature values of one credible set, by gene."""
    return {
        row["geneId"]: float(row["featureValue"])
        for row in feature.df.filter(f.col("studyLocusId") == study_locus_id).collect()
    }


class TestFmPopsTarget:
    """Test the target and covariates fmPops is fitted on.

    Three protein-coding genes and one lncRNA on chromosome 1. Variant v1 is nearest to geneA
    once the lncRNA is set aside, and variant v2 is as close to geneA as to geneC.
    """

    @pytest.fixture()
    def target_index(self: TestFmPopsTarget, spark: SparkSession) -> TargetIndex:
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
    def variant_index(self: TestFmPopsTarget, spark: SparkSession) -> VariantIndex:
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
    def study_index(self: TestFmPopsTarget, spark: SparkSession) -> StudyIndex:
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
    def study_locus(self: TestFmPopsTarget, spark: SparkSession) -> StudyLocus:
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
        self: TestFmPopsTarget,
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

    def test_gene_covariates(self: TestFmPopsTarget, target_index: TargetIndex) -> None:
        """Gene length and the number of neighbouring protein-coding genes, logged."""
        observed = {
            row["geneId"]: (row["logGeneLength"], row["logNeighbouringGenes"])
            for row in gene_covariates(target_index).collect()
        }
        assert set(observed) == {"geneA", "geneB", "geneC"}
        assert observed["geneA"] == pytest.approx((math.log(10_000), math.log(3)))
        assert observed["geneB"] == pytest.approx((math.log(1_000), math.log(3)))
        nearby = gene_covariates(target_index, genomic_window=50_000)
        assert {
            row["geneId"]: row["logNeighbouringGenes"] for row in nearby.collect()
        } == pytest.approx({"geneA": math.log(2), "geneB": 0.0, "geneC": math.log(2)})


class TestFmPopsFeatureLogic:
    """Test how the gene scores become features, with the scores fixed in advance.

    The scores cover gene1 (0.6), gene2 (-0.2), gene3 (-0.4) and gene5 (2.0). gene4 is
    protein coding and within the window but has no score, gene5 has a non-coding biotype, and
    gene6 is too far away. sl1 sits among the first five genes; sl2 only reaches gene3.
    """

    @pytest.fixture()
    def dependencies(
        self: TestFmPopsFeatureLogic, spark: SparkSession
    ) -> dict[str, Any]:
        """Fitted scores, two credible sets and the genes around them."""
        pops_gene_features = PopsGeneFeatures("unused")
        pops_gene_features._scores = spark.createDataFrame(
            [("gene1", 0.6), ("gene2", -0.2), ("gene3", -0.4), ("gene5", 2.0)],
            "geneId string, fmPops double",
        )
        study_locus = StudyLocus(
            _df=spark.createDataFrame(
                [("sl1", "var1", "study1", 2000), ("sl2", "var2", "study1", 502_500)],
                "studyLocusId string, variantId string, studyId string, position integer",
            ).withColumn("chromosome", f.lit("1")),
            _schema=StudyLocus.get_schema(),
        )
        target_index = _target_index(
            spark,
            [
                ("gene1", "protein_coding", "1", 1000, 1999, 1000),
                ("gene2", "protein_coding", "1", 2000, 2999, 2000),
                ("gene3", "protein_coding", "1", 3000, 3999, 3000),
                ("gene4", "protein_coding", "1", 2500, 2999, 2500),
                ("gene5", "lncRNA", "1", 2500, 2999, 2500),
                ("gene6", "protein_coding", "1", 10_000_000, 10_000_999, 10_000_000),
            ],
        )
        return {
            "pops_gene_features": pops_gene_features,
            "study_locus": study_locus,
            "study_index": None,
            "variant_index": None,
            "target_index": target_index,
        }

    def test_local_feature_is_the_gene_score(
        self: TestFmPopsFeatureLogic, dependencies: dict[str, Any]
    ) -> None:
        """One row per credible set and scored protein-coding gene in the window."""
        feature = FmPopsFeature.compute(
            study_loci_to_annotate=dependencies["study_locus"],
            feature_dependency=dependencies,
        )
        assert _scores(feature, "sl1") == {
            "gene1": pytest.approx(0.6),
            "gene2": pytest.approx(-0.2),
            "gene3": pytest.approx(-0.4),
        }
        assert _scores(feature, "sl2") == {"gene3": pytest.approx(-0.4)}
        assert feature.df.count() == 4

    def test_neighbourhood_is_scaled_between_the_locus_minimum_and_maximum(
        self: TestFmPopsFeatureLogic, dependencies: dict[str, Any]
    ) -> None:
        """The best gene at a locus gets 1, the worst 0, and a lone gene 1."""
        feature = FmPopsNeighbourhoodFeature.compute(
            study_loci_to_annotate=dependencies["study_locus"],
            feature_dependency=dependencies,
        )
        assert _scores(feature, "sl1") == {
            "gene1": pytest.approx(1.0),
            "gene2": pytest.approx(0.2),
            "gene3": pytest.approx(0.0),
        }
        assert _scores(feature, "sl2") == {"gene3": pytest.approx(1.0)}
        assert {
            row["featureName"] for row in feature.df.select("featureName").collect()
        } == {"fmPopsNeighbourhood"}


class TestFmPopsEndToEnd:
    """Fit fmPops from toy PoPS feature files through the feature factory.

    Twelve genes 200 kb apart on each of chromosomes 1, 2 and 6, those on chromosome 6 inside
    the MHC. Every gene is the nearest gene of one credible set, whose study carries zero to
    three diseases. The PoPS features cover every gene but the last and one gene the release
    does not have.
    """

    CHROMOSOMES = {"1": 1_000_000, "2": 1_000_000, "6": 29_000_000}
    GENES_PER_CHROMOSOME = 12

    @pytest.fixture()
    def genes(self: TestFmPopsEndToEnd) -> list[tuple[str, str, int]]:
        """Gene identifier, chromosome and TSS of every toy gene."""
        return [
            (f"chr{chromosome}_gene{i}", chromosome, first_tss + 200_000 * i)
            for chromosome, first_tss in self.CHROMOSOMES.items()
            for i in range(self.GENES_PER_CHROMOSOME)
        ]

    @pytest.fixture()
    def release(
        self: TestFmPopsEndToEnd,
        spark: SparkSession,
        tmp_path: Path,
        genes: list[tuple[str, str, int]],
    ) -> dict[str, Any]:
        """Datasets of the toy release and the PoPS feature directory."""
        rng = np.random.default_rng(3)
        variants, credible_sets, studies = [], [], []
        for i, (gene, chromosome, tss) in enumerate(genes):
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

        feature_dir = tmp_path / "pops"
        matrix_dir = feature_dir / "munged_features"
        matrix_dir.mkdir(parents=True)
        rows = [gene for gene, _, _ in genes[:-1]] + ["ENSG_not_in_release"]
        (matrix_dir / "pops_features.rows.txt").write_text("\n".join(rows) + "\n")
        (feature_dir / "control.features").write_text("expr.control\n")
        for i, columns in enumerate(
            [["expr.1", "expr.2", "expr.control"], ["ppi.1", "ppi.2", "ppi.3"]]
        ):
            (matrix_dir / f"pops_features.cols.{i}.txt").write_text(
                "\n".join(columns) + "\n"
            )
            np.save(
                matrix_dir / f"pops_features.mat.{i}.npy",
                rng.normal(size=(len(rows), len(columns))),
            )

        return {
            "pops_gene_features": PopsGeneFeatures(
                str(feature_dir), lambda_grid=[0.1, 1.0, 10.0]
            ),
            "study_locus": StudyLocus(
                _df=spark.createDataFrame(
                    credible_sets,
                    "studyLocusId string, studyId string, variantId string, "
                    "chromosome string, position integer, studyType string",
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
            "target_index": _target_index(
                spark,
                [
                    (gene, "protein_coding", chromosome, tss, tss + 999, tss)
                    for gene, chromosome, tss in genes
                ],
            ),
        }

    def test_feature_factory_fits_once_and_builds_both_features(
        self: TestFmPopsEndToEnd,
        release: dict[str, Any],
        genes: list[tuple[str, str, int]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both features come from a single fit and cover the scored genes in each window."""
        fits = []
        fit = PopsGeneFeatures._fit

        def counting_fit(self: PopsGeneFeatures, *args: Any) -> Any:
            fits.append(1)
            return fit(self, *args)

        monkeypatch.setattr(PopsGeneFeatures, "_fit", counting_fit)

        local, neighbourhood = FeatureFactory(
            release["study_locus"], ["fmPops", "fmPopsNeighbourhood"]
        ).generate_features(L2GFeatureInputLoader(**release))

        assert len(fits) == 1
        local_rows = local.df.toPandas()
        neighbourhood_rows = neighbourhood.df.toPandas()
        # Genes are 200 kb apart, so each credible set reaches up to five genes, and the last
        # gene, which has no PoPS features, never appears.
        unscored = genes[-1][0]
        assert unscored not in set(local_rows["geneId"])
        assert len(local_rows) == len(neighbourhood_rows)
        assert local_rows.groupby("studyLocusId").size().max() == 5
        assert np.isfinite(local_rows["featureValue"]).all()
        assert (local_rows["featureValue"] != 0).any()
        assert neighbourhood_rows["featureValue"].between(0.0, 1.0).all()
        assert (
            neighbourhood_rows.groupby("studyLocusId")["featureValue"].max() == 1.0
        ).all()
