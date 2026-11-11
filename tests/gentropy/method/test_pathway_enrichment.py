"""Test the pathway library and its enrichment regression."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest
import scipy.sparse

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
        }

    def test_gene_sets_outside_the_size_limits_are_dropped(
        self, mock_pathway_library: PathwayLibrary, target_index: TargetIndex
    ) -> None:
        """Only the three GO terms shared by g1 and g2 have two members."""
        pathways = {
            row["pathwayId"]
            for row in mock_pathway_library.gene_sets(
                target_index, min_size=2, max_size=2
            ).collect()
        }
        assert pathways == {"GO:0000001", "GO:0000002", "GO:0000003"}


def test_regression_matches_a_direct_fit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Slopes and region-clustered standard errors equal those of each full model fitted alone.

    The covariates include a constant column, which adds no direction, and one pathway has no
    member, so it has no slope. A small block size makes the pathways run in several blocks.
    """
    rng = np.random.default_rng(0)
    n_genes, n_pathways, n_units = 300, 7, 4
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
    outcomes = (rng.random((n_genes, n_units)) < 0.1).astype(float)
    regions = rng.integers(0, 40, n_genes)
    monkeypatch.setattr(pathway_enrichment, "_BLOCK_BYTES", 8 * n_genes * 3)

    beta, standard_error = pathway_enrichment._pathway_regression(
        covariates,
        scipy.sparse.csc_matrix(membership),
        scipy.sparse.csc_matrix(outcomes),
        regions,
    )

    assert np.isnan(beta[:, -1]).all() and np.isnan(standard_error[:, -1]).all()
    for unit in range(n_units):
        for pathway in range(n_pathways - 1):
            design = np.column_stack([covariates[:, :3], membership[:, pathway]])
            coefficients = np.linalg.lstsq(design, outcomes[:, unit], rcond=None)[0]
            residuals = outcomes[:, unit] - design @ coefficients
            bread = np.linalg.inv(design.T @ design)
            scores = [
                design[regions == region].T @ residuals[regions == region]
                for region in np.unique(regions)
            ]
            variance = bread @ sum(np.outer(score, score) for score in scores) @ bread
            assert beta[unit, pathway] == pytest.approx(coefficients[-1])
            assert standard_error[unit, pathway] == pytest.approx(
                np.sqrt(variance[-1, -1])
            )


def test_benjamini_hochberg_ignores_missing_p_values() -> None:
    """Each row is adjusted over its own finite p-values only."""
    adjusted = pathway_enrichment._benjamini_hochberg(
        np.array([[0.01, 0.04, np.nan, 0.03], [0.5, np.nan, np.nan, np.nan]])
    )
    np.testing.assert_allclose(
        adjusted, [[0.03, 0.04, np.nan, 0.04], [0.5, np.nan, np.nan, np.nan]]
    )


class TestRegression:
    """Test the enrichment regression on a toy release.

    Forty protein-coding genes, 10 Mb apart so that each is its own region: gene0-gene19 on
    chromosome 1 and gene20-gene39 on chromosome 2, all of the same length. PA holds gene0-gene7
    and gene20-gene27, PB the other genes, so every gene is in one pathway. disease1 lists six
    PA genes on each chromosome; disease2 lists three genes on chromosome 1 only.
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

        On the 20 remaining genes, disease1 lists 6 of the 8 PA genes and none of the others:
        beta = 0.75 and, each gene being its own region, SE = sqrt(0.54) / 4.8.
        """
        rows = PathwayLibrary.regression(*inputs, min_genes=4).collect()
        assert {
            (r["diseaseId"], r["heldOutChromosome"], r["pathwayId"]) for r in rows
        } == {
            ("disease1", "1", "PA"),
            ("disease1", "2", "PA"),
        }
        for row in rows:
            assert row["beta"] == pytest.approx(0.75)
            assert row["betaError"] == pytest.approx(np.sqrt(0.54) / 4.8)
            assert row["pValueAdjusted"] == pytest.approx(min(2 * row["pValue"], 1.0))

    def test_diseases_with_too_few_genes_left_are_not_tested(
        self, inputs: tuple[DataFrame, DataFrame, TargetIndex]
    ) -> None:
        """With a minimum of three genes, disease2 is tested only when chromosome 2 is held out."""
        rows = PathwayLibrary.regression(
            *inputs, min_genes=3, p_value_adjusted_threshold=1.01
        ).collect()
        assert {(r["diseaseId"], r["heldOutChromosome"]) for r in rows} == {
            ("disease1", "1"),
            ("disease1", "2"),
            ("disease2", "2"),
        }

    def test_only_the_requested_chromosomes_are_held_out(
        self, inputs: tuple[DataFrame, DataFrame, TargetIndex]
    ) -> None:
        """Holding out chromosome 2 only returns its results."""
        rows = PathwayLibrary.regression(
            *inputs, min_genes=4, held_out_chromosomes=["2"]
        ).collect()
        assert {r["heldOutChromosome"] for r in rows} == {"2"}
