"""Test the pathway library and its over-representation test."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from scipy.stats import hypergeom

from gentropy.dataset.target_index import TargetIndex
from gentropy.method.pathway_enrichment import PathwayLibrary

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


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


class TestOverRepresentation:
    """Test the hypergeometric test.

    Ten library genes in four pathways: PA holds G1-G4, PB G5-G8, PC G9-G10 and PD G1 and G5.
    disease1 has G1-G3 and G11, which no pathway contains; disease2 has G9 only.
    """

    @pytest.fixture()
    def results(self, spark: SparkSession) -> dict[str, dict[str, Any]]:
        """Test results of disease1 by pathway, with diseases of two or more genes tested."""
        members = {
            "PA": ["G1", "G2", "G3", "G4"],
            "PB": ["G5", "G6", "G7", "G8"],
            "PC": ["G9", "G10"],
            "PD": ["G1", "G5"],
        }
        gene_sets = spark.createDataFrame(
            [(pathway, gene) for pathway, genes in members.items() for gene in genes],
            "pathwayId string, geneId string",
        )
        gene_lists = spark.createDataFrame(
            [
                ("disease1", "G1"),
                ("disease1", "G2"),
                ("disease1", "G3"),
                ("disease1", "G11"),
                ("disease2", "G9"),
            ],
            "diseaseId string, geneId string",
        )
        rows = PathwayLibrary.over_representation(
            gene_lists, gene_sets, min_genes=2
        ).collect()
        assert {row["diseaseId"] for row in rows} == {"disease1"}
        return {row["pathwayId"]: row.asDict() for row in rows}

    def test_only_overlapping_pathways_are_returned(
        self, results: dict[str, dict[str, Any]]
    ) -> None:
        """PB and PC share no gene with disease1."""
        assert set(results) == {"PA", "PD"}

    def test_counts_exclude_genes_outside_the_library(
        self, results: dict[str, dict[str, Any]]
    ) -> None:
        """G11 is in no pathway, so disease1 counts three genes."""
        assert {
            key: results["PA"][key]
            for key in ("overlap", "pathwaySize", "diseaseGeneCount", "backgroundSize")
        } == {
            "overlap": 3,
            "pathwaySize": 4,
            "diseaseGeneCount": 3,
            "backgroundSize": 10,
        }

    def test_p_values_are_the_hypergeometric_upper_tail(
        self, results: dict[str, dict[str, Any]]
    ) -> None:
        """P(X >= k) with N = 10 library genes and n = 3 disease genes."""
        assert results["PA"]["pValue"] == pytest.approx(hypergeom.sf(2, 10, 4, 3))
        assert results["PD"]["pValue"] == pytest.approx(hypergeom.sf(0, 10, 2, 3))

    def test_adjustment_counts_every_pathway_of_the_library(
        self, results: dict[str, dict[str, Any]]
    ) -> None:
        """Benjamini-Hochberg over m = 4 pathways, although only two are returned."""
        p_a, p_d = results["PA"]["pValue"], results["PD"]["pValue"]
        assert results["PA"]["pValueAdjusted"] == pytest.approx(
            min(p_a * 4 / 1, p_d * 4 / 2, 1.0)
        )
        assert results["PD"]["pValueAdjusted"] == pytest.approx(min(p_d * 4 / 2, 1.0))
