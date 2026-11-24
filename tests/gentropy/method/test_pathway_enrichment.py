"""Test the pathway library and its enrichment score test."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest
import scipy.sparse
from scipy.special import expit
from scipy.stats import binom, norm

from gentropy.dataset.target_index import TargetIndex
from gentropy.method import pathway_enrichment
from gentropy.method.pathway_enrichment import PathwayLibrary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _gene(
    gene_id: str,
    go: list[tuple[str, str, str]] | None = None,
    pathways: list[str] | None = None,
    biotype: str = "protein_coding",
) -> dict[str, Any]:
    """Target index row with GO annotations as (id, evidence, aspect) and Reactome ids."""
    return {
        "id": gene_id,
        "biotype": biotype,
        "go": [
            {"id": term, "evidence": evidence, "aspect": aspect}
            for term, evidence, aspect in go or []
        ],
        "pathways": [{"pathwayId": pathway} for pathway in pathways or []],
    }


class TestPathwayLibrary:
    """Test how gene sets are built from the release, with the `mock_pathway_library` ontologies."""

    @pytest.fixture()
    def target_index(self, spark: SparkSession) -> TargetIndex:
        """Genes that each exercise one rule of the library."""
        rows = [
            _gene("g1", go=[("GO:0000003", "IDA", "P")]),
            # Annotated through the alternative identifier of GO:0000003
            _gene("g2", go=[("GO:0000033", "IMP", "P")]),
            # GO:0000004 only regulates GO:0000002, which is not followed
            _gene("g3", go=[("GO:0000004", "IDA", "P")]),
            # Electronic annotation
            _gene("g4", go=[("GO:0000002", "IEA", "P")]),
            # A cellular component and an obsolete term
            _gene("g5", go=[("GO:0000009", "IDA", "C"), ("GO:0000005", "IDA", "P")]),
            _gene("g6", pathways=["R-HSA-2"]),
            _gene("g7", go=[("GO:0000001", "IDA", "P")], biotype="lncRNA"),
            # Keep every pathway's members distinct, so none is collapsed
            _gene("g8", go=[("GO:0000002", "IDA", "P")]),
            _gene("g9", go=[("GO:0000001", "IDA", "P")]),
            _gene("g10", pathways=["R-HSA-3"]),
        ]
        return TargetIndex(
            _df=spark.createDataFrame(rows, TargetIndex.get_schema()),
            _schema=TargetIndex.get_schema(),
        )

    def test_go_terms_map_to_their_is_a_and_part_of_ancestors(
        self, mock_pathway_library: PathwayLibrary
    ) -> None:
        """GO:0000003 reaches GO:0000001 through GO:0000002; regulates is not followed."""
        ancestors = {
            (row["termId"], row["pathwayId"])
            for row in mock_pathway_library.go_term_ancestors().collect()
        }
        assert ancestors == {
            ("GO:0000001", "GO:0000001"),
            ("GO:0000002", "GO:0000002"),
            ("GO:0000002", "GO:0000001"),
            ("GO:0000003", "GO:0000003"),
            ("GO:0000003", "GO:0000002"),
            ("GO:0000003", "GO:0000001"),
            ("GO:0000033", "GO:0000003"),
            ("GO:0000033", "GO:0000002"),
            ("GO:0000033", "GO:0000001"),
            ("GO:0000004", "GO:0000004"),
        }

    def test_gene_sets(
        self, mock_pathway_library: PathwayLibrary, target_index: TargetIndex
    ) -> None:
        """Annotations are propagated, and only protein-coding non-electronic BP ones count."""
        gene_sets = {
            (row["pathwayId"], row["geneId"])
            for row in mock_pathway_library.gene_sets(
                target_index, min_size=1, max_size=10
            ).collect()
        }
        assert gene_sets == {
            ("GO:0000003", "g1"),
            ("GO:0000002", "g1"),
            ("GO:0000001", "g1"),
            ("GO:0000003", "g2"),
            ("GO:0000002", "g2"),
            ("GO:0000001", "g2"),
            ("GO:0000004", "g3"),
            ("R-HSA-2", "g6"),
            ("R-HSA-1", "g6"),
            ("GO:0000002", "g8"),
            ("GO:0000001", "g8"),
            ("GO:0000001", "g9"),
            ("R-HSA-3", "g10"),
            ("R-HSA-1", "g10"),
        }

    def test_gene_sets_outside_the_size_limits_are_dropped(
        self, mock_pathway_library: PathwayLibrary, target_index: TargetIndex
    ) -> None:
        """Only GO:0000003 (g1, g2) and R-HSA-1 (g6, g10) have two members."""
        pathways = {
            row["pathwayId"]
            for row in mock_pathway_library.gene_sets(
                target_index, min_size=2, max_size=2
            ).collect()
        }
        assert pathways == {"GO:0000003", "R-HSA-1"}

    def test_identical_gene_sets_collapse_to_the_lowest_id(
        self, spark: SparkSession, mock_pathway_library: PathwayLibrary
    ) -> None:
        """Identical sets collapse across GO and Reactome; a near-duplicate stays.

        gA and gB are in GO:0000003 and R-HSA-2, so GO:0000001-3 and R-HSA-2 all hold exactly
        gA and gB and collapse to GO:0000001. gE is in R-HSA-3, so R-HSA-1 holds gA, gB and gE
        and is kept.
        """
        target_index = TargetIndex(
            _df=spark.createDataFrame(
                [
                    _gene("gA", go=[("GO:0000003", "IDA", "P")], pathways=["R-HSA-2"]),
                    _gene("gB", go=[("GO:0000003", "IDA", "P")], pathways=["R-HSA-2"]),
                    _gene("gE", pathways=["R-HSA-3"]),
                ],
                TargetIndex.get_schema(),
            ),
            _schema=TargetIndex.get_schema(),
        )
        gene_sets = {
            (row["pathwayId"], row["geneId"])
            for row in mock_pathway_library.gene_sets(
                target_index, min_size=1, max_size=10
            ).collect()
        }
        assert gene_sets == {
            ("GO:0000001", "gA"),
            ("GO:0000001", "gB"),
            ("R-HSA-1", "gA"),
            ("R-HSA-1", "gB"),
            ("R-HSA-1", "gE"),
            ("R-HSA-3", "gE"),
        }


def test_score_test_matches_a_direct_computation() -> None:
    """Score and efficient information equal a dense computation from the fitted model.

    The covariates include a constant column, which adds no direction, and one pathway has no
    member, so it cannot be tested.
    """
    rng = np.random.default_rng(0)
    n_genes, n_pathways = 400, 6
    covariates = np.column_stack(
        [
            np.ones(n_genes),
            rng.normal(size=n_genes),
            rng.poisson(5, n_genes),
            np.full(n_genes, 3.0),
        ]
    )
    membership = (rng.random((n_genes, n_pathways)) < 0.2).astype(float)
    membership[:, -1] = 0.0
    outcome = (rng.random(n_genes) < expit(-2 + 0.5 * covariates[:, 1])).astype(float)

    z_score, p_value = pathway_enrichment._score_test(
        covariates, scipy.sparse.csc_matrix(membership), outcome, saddlepoint_z=np.inf
    )

    fitted = pathway_enrichment._logistic_fit(covariates, outcome)
    np.testing.assert_allclose(covariates.T @ (outcome - fitted), 0.0, atol=1e-6)
    weight = np.diag(fitted * (1 - fitted))
    adjusted = membership - covariates @ np.linalg.pinv(
        covariates.T @ weight @ covariates
    ) @ (covariates.T @ weight @ membership)
    expected = (adjusted.T @ (outcome - fitted))[:-1] / np.sqrt(
        np.diag(adjusted.T @ weight @ adjusted)[:-1]
    )
    np.testing.assert_allclose(z_score[:-1], expected, rtol=1e-6)
    np.testing.assert_allclose(p_value[:-1], norm.sf(expected), rtol=1e-6)
    assert np.isnan(z_score[-1]) and np.isnan(p_value[-1])


def test_saddlepoint_matches_the_exact_tail() -> None:
    """A 5-gene pathway with every member listed, as in a small, rare overlap.

    With an intercept only, every gene has the same probability p and the score is
    `(1 - m) K_in - m K_out - E`, with `K_in ~ Bin(5, p)` and `K_out ~ Bin(n - 5, p)`
    independent and `m = 5 / n`, so its upper tail can be summed exactly. The normal
    approximation is far off; the saddlepoint is not.
    """
    n_genes, size, listed = 2000, 5, 30
    outcome = np.zeros(n_genes)
    outcome[:listed] = 1.0
    membership = np.zeros((n_genes, 1))
    membership[:size, 0] = 1.0
    covariates = np.ones((n_genes, 1))

    _, p_normal = pathway_enrichment._score_test(
        covariates, scipy.sparse.csc_matrix(membership), outcome, saddlepoint_z=np.inf
    )
    _, p_saddle = pathway_enrichment._score_test(
        covariates, scipy.sparse.csc_matrix(membership), outcome, saddlepoint_z=2.0
    )

    rate, share = outcome.mean(), size / n_genes
    observed = (1 - share) * size - share * (listed - size)
    k_in = np.arange(size + 1)
    k_out = np.arange(n_genes - size + 1)
    statistic = (1 - share) * k_in[:, None] - share * k_out[None, :]
    probability = (
        binom.pmf(k_in, size, rate)[:, None]
        * binom.pmf(k_out, n_genes - size, rate)[None, :]
    )
    exact = probability[statistic >= observed - 1e-9].sum()

    # Within a factor of two: the score is discrete and no continuity correction is applied.
    assert exact / 2 < p_saddle[0] < exact * 2
    assert p_normal[0] < exact / 100


def test_score_test_is_calibrated_on_random_lists() -> None:
    """Random lists drawn from the covariates alone: about 5% of tests below 0.05."""
    rng = np.random.default_rng(1)
    n_genes, n_pathways = 3000, 300
    covariates = np.column_stack([np.ones(n_genes), rng.normal(size=n_genes)])
    sizes = rng.integers(5, 200, n_pathways)
    membership = np.zeros((n_genes, n_pathways))
    for pathway, size in enumerate(sizes):
        membership[rng.choice(n_genes, size, replace=False), pathway] = 1.0
    sparse_membership = scipy.sparse.csc_matrix(membership)
    p_values = []
    for _ in range(20):
        outcome = (rng.random(n_genes) < expit(-3 + covariates[:, 1])).astype(float)
        p_values.append(
            pathway_enrichment._score_test(
                covariates, sparse_membership, outcome, saddlepoint_z=2.0
            )[1]
        )
    share = np.nanmean(np.array(p_values) < 0.05)
    assert 0.03 < share < 0.07


def test_gene_covariates_count_protein_coding_tss_only(spark: SparkSession) -> None:
    """A lncRNA next to g1 counts towards no gene's density."""
    rows = [
        {
            "id": gene_id,
            "biotype": biotype,
            "genomicLocation": {"chromosome": "1", "start": tss, "end": tss + 999},
            "tss": tss,
        }
        for gene_id, biotype, tss in [
            ("g1", "protein_coding", 1_000_000),
            ("g2", "protein_coding", 1_500_000),
            ("g3", "lncRNA", 1_100_000),
            ("g4", "protein_coding", 5_000_000),
        ]
    ]
    target_index = TargetIndex(
        _df=spark.createDataFrame(rows, TargetIndex.get_schema()),
        _schema=TargetIndex.get_schema(),
    )
    genes = pathway_enrichment.gene_covariates(target_index, 1_000_000)
    assert genes["geneId"].tolist() == ["g1", "g2", "g4"]
    np.testing.assert_allclose(genes["logDensity"], np.log1p([2, 2, 1]))
    np.testing.assert_allclose(genes["logLength"], np.log10(999))


class TestScoreTest:
    """Test the enrichment score test on a toy release.

    Forty protein-coding genes, 10 Mb apart: gene0-gene19 on chromosome 1 and gene20-gene39 on
    chromosome 2, all of the same length. PA holds gene0-gene7 and gene20-gene27, PB the other
    genes, so every gene is in one pathway. disease1 lists six PA genes on each chromosome;
    disease2 lists three genes on chromosome 1 only.
    """

    @pytest.fixture()
    def inputs(self, spark: SparkSession) -> tuple[DataFrame, DataFrame, TargetIndex]:
        """Gene lists, gene sets and target index."""
        genes = [f"gene{i}" for i in range(40)]
        target_index = TargetIndex(
            _df=spark.createDataFrame(
                [
                    {
                        "id": gene,
                        "biotype": "protein_coding",
                        "genomicLocation": {
                            "chromosome": "1" if i < 20 else "2",
                            "start": 10_000_000 * (i + 1),
                            "end": 10_000_000 * (i + 1) + 999,
                        },
                        "tss": 10_000_000 * (i + 1),
                    }
                    for i, gene in enumerate(genes)
                ],
                TargetIndex.get_schema(),
            ),
            _schema=TargetIndex.get_schema(),
        )
        gene_sets = spark.createDataFrame(
            [("PA" if i % 20 < 8 else "PB", gene) for i, gene in enumerate(genes)],
            "pathwayId string, geneId string",
        )
        gene_lists = spark.createDataFrame(
            [
                ("disease1", "1" if i < 20 else "2", f"gene{i}")
                for i in [*range(6), *range(20, 26)]
            ]
            + [("disease2", "1", f"gene{i}") for i in (0, 10, 15)],
            "diseaseId string, chromosome string, geneId string",
        )
        return gene_lists, gene_sets, target_index

    def test_enriched_pathways_per_held_out_chromosome(
        self, inputs: tuple[DataFrame, DataFrame, TargetIndex]
    ) -> None:
        """PA is enriched for disease1 whichever chromosome is held out; PB is depleted.

        On the 20 remaining genes, disease1 lists 6 of the 8 PA genes and none of the others.
        """
        rows = PathwayLibrary.score_test(*inputs, min_genes=4).collect()
        assert {
            (r["diseaseId"], r["heldOutChromosome"], r["pathwayId"]) for r in rows
        } == {
            ("disease1", "1", "PA"),
            ("disease1", "2", "PA"),
        }
        for row in rows:
            assert row["zScore"] > 0
            assert row["pValueAdjusted"] == pytest.approx(min(2 * row["pValue"], 1.0))

    def test_minimum_genes_counts_every_chromosome(
        self, inputs: tuple[DataFrame, DataFrame, TargetIndex]
    ) -> None:
        """disease1 lists six genes per held-out chromosome but twelve in all, so a minimum of seven still tests it on both; disease2, with three, is not tested."""
        rows = PathwayLibrary.score_test(
            *inputs, min_genes=7, p_value_adjusted_threshold=1.01
        ).collect()
        assert {(r["diseaseId"], r["heldOutChromosome"]) for r in rows} == {
            ("disease1", "1"),
            ("disease1", "2"),
        }

    def test_only_the_requested_chromosomes_are_held_out(
        self, inputs: tuple[DataFrame, DataFrame, TargetIndex]
    ) -> None:
        """Holding out chromosome 2 only returns its results."""
        rows = PathwayLibrary.score_test(
            *inputs, min_genes=4, held_out_chromosomes=["2"]
        ).collect()
        assert {r["heldOutChromosome"] for r in rows} == {"2"}
