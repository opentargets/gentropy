"""Test the expression specificity catalogue and its biosample enrichment test."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest
from scipy.stats import norm

from gentropy.dataset.target_index import TargetIndex
from gentropy.method.biosample_enrichment import ExpressionSpecificity

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

BASELINE_EXPRESSION_SCHEMA = (
    "targetId string, datasourceId string, tissueBiosampleId string, "
    "celltypeBiosampleId string, specificity_score double"
)


def _score_test(x: np.ndarray, y: np.ndarray) -> float:
    """Score statistic of the slope of a logistic regression of y on x, from the information matrix.

    Gradient and Fisher information of the log-likelihood at the intercept-only fit, with the
    efficient information of the slope after adjusting for the intercept.
    """
    p = y.mean()
    design = np.column_stack([np.ones_like(x), x])
    gradient = design.T @ (y - p)
    information = p * (1 - p) * design.T @ design
    efficient = information[1, 1] - information[1, 0] ** 2 / information[0, 0]
    return float(gradient[1] / np.sqrt(efficient))


class TestExpressionSpecificity:
    """Ten protein-coding genes g0-g9 and a lncRNA, scored in three biosamples of one datasource.

    The disease's genes are g0-g3 and the lncRNA on chromosome 1 and g4 on chromosome 2, so
    holding out chromosome 2 leaves g0-g3 and the lncRNA. In tissue A g0-g3 score highest, in
    tissue B lowest, and in tissue C every gene scores 0.
    """

    genes = [f"g{i}" for i in range(10)]
    tissue_a = np.array([0.9, 0.8, 0.7, 0.6, 0.1, 0.0, 0.0, 0.2, 0.0, 0.0])
    tissue_b = np.array([0.0, 0.0, 0.0, 0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 0.4])
    is_listed = np.array([1.0] * 4 + [0.0] * 6)

    @pytest.fixture()
    def target_index(self, spark: SparkSession) -> TargetIndex:
        """Protein-coding genes g0-g9 and the lncRNA nc1."""
        rows = [{"id": gene, "biotype": "protein_coding"} for gene in self.genes]
        rows.append({"id": "nc1", "biotype": "lncRNA"})
        return TargetIndex(
            _df=spark.createDataFrame(rows, TargetIndex.get_schema()),
            _schema=TargetIndex.get_schema(),
        )

    @pytest.fixture()
    def expression_specificity(self, spark: SparkSession) -> ExpressionSpecificity:
        """Three tissues, plus proteomics rows that carry no specificity score."""
        rows: list[tuple[Any, ...]] = []
        for tissue, scores in [
            ("UBERON_A", self.tissue_a),
            ("UBERON_B", self.tissue_b),
            ("UBERON_C", np.zeros(10)),
        ]:
            rows += [
                (gene, "source1", tissue, None, float(score))
                for gene, score in zip(self.genes, scores)
            ]
            rows.append(("nc1", "source1", tissue, None, 1.0))
        rows += [(gene, "proteomics", "UBERON_A", None, None) for gene in self.genes]
        return ExpressionSpecificity(
            baseline_expression=spark.createDataFrame(rows, BASELINE_EXPRESSION_SCHEMA)
        )

    @pytest.fixture()
    def gene_lists(self, spark: SparkSession) -> DataFrame:
        """disease1: g0-g3 and the lncRNA on chromosome 1, g4 on chromosome 2."""
        return spark.createDataFrame(
            [("disease1", "1", gene) for gene in ["g0", "g1", "g2", "g3", "nc1"]]
            + [("disease1", "2", "g4")],
            "diseaseId string, chromosome string, geneId string",
        )

    def _results(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
        held_out_chromosome: str = "2",
        **kwargs: Any,
    ) -> dict[str, dict[str, Any]]:
        return {
            row["biosampleId"]: row.asDict()
            for row in expression_specificity.enrichment(
                gene_lists, target_index, **kwargs
            ).collect()
            if row["heldOutChromosome"] == held_out_chromosome
        }

    def test_specificity_ids_a_cell_type_within_a_tissue_as_null(
        self, spark: SparkSession
    ) -> None:
        """A tissue or a cell type keeps its id; a cell type within a tissue has none."""
        rows = [
            ("g0", "source1", "UBERON_A", None, 0.5),
            ("g0", "source1", None, "CL_A", 0.5),
            ("g0", "source1", "UBERON_A", "CL_A", 0.5),
        ]
        specificity = ExpressionSpecificity(
            baseline_expression=spark.createDataFrame(rows, BASELINE_EXPRESSION_SCHEMA)
        ).specificity()
        assert sorted((row["biosampleId"] or "") for row in specificity.collect()) == [
            "",
            "CL_A",
            "UBERON_A",
        ]

    def test_z_score_is_the_logistic_regression_score_statistic(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """The lncRNA is not a protein-coding gene, so m = 4 of n = 10 genes."""
        results = self._results(
            expression_specificity, gene_lists, target_index, min_genes=4
        )
        tissue_a = results["UBERON_A"]
        assert (tissue_a["diseaseGeneCount"], tissue_a["geneCount"]) == (4, 10)
        assert tissue_a["zScore"] == pytest.approx(
            _score_test(self.tissue_a, self.is_listed)
        )
        assert tissue_a["pValue"] == pytest.approx(norm.sf(tissue_a["zScore"]))

    def test_genes_of_the_held_out_chromosome_are_left_out(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """Holding out chromosome 1 leaves g4 alone; with four genes required only chromosome 2 is held out."""
        tissue_a = self._results(
            expression_specificity,
            gene_lists,
            target_index,
            held_out_chromosome="1",
            min_genes=1,
        )["UBERON_A"]
        assert tissue_a["diseaseGeneCount"] == 1
        assert tissue_a["zScore"] == pytest.approx(
            _score_test(self.tissue_a, np.eye(10)[4])
        )
        assert {
            row["heldOutChromosome"]
            for row in expression_specificity.enrichment(
                gene_lists, target_index, min_genes=4
            ).collect()
        } == {"2"}

    def test_negative_slope_has_a_p_value_of_one(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """In tissue B the disease's genes score lowest."""
        tissue_b = self._results(
            expression_specificity, gene_lists, target_index, min_genes=4
        )["UBERON_B"]
        assert tissue_b["zScore"] < 0
        assert tissue_b["pValue"] == 1.0

    def test_benjamini_hochberg_over_the_testable_biosamples(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """Tissue C has no variance and the proteomics rows no scores, so two tests remain."""
        results = self._results(
            expression_specificity, gene_lists, target_index, min_genes=4
        )
        assert set(results) == {"UBERON_A", "UBERON_B"}
        assert results["UBERON_A"]["pValueAdjusted"] == pytest.approx(
            2 * results["UBERON_A"]["pValue"]
        )
        assert results["UBERON_B"]["pValueAdjusted"] == 1.0

    def test_diseases_with_too_few_genes_are_not_tested(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """The lncRNA does not count towards the four protein-coding genes of disease1."""
        assert (
            self._results(expression_specificity, gene_lists, target_index, min_genes=5)
            == {}
        )
