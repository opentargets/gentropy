"""Test the expression specificity panels and their biosample enrichment regression."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest
from scipy.stats import norm

from gentropy.dataset.target_index import TargetIndex
from gentropy.method.biosample_enrichment import (
    ExpressionSpecificity,
    SpecificityPanel,
)
from gentropy.method.pathway_enrichment import benjamini_hochberg

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

BASELINE_EXPRESSION_SCHEMA = (
    "targetId string, datasourceId string, tissueBiosampleId string, "
    "celltypeBiosampleId string, specificity_score double"
)
PANELS = (
    SpecificityPanel("source1", "tissue"),
    SpecificityPanel("source2", "celltype"),
)


def _clustered_ols(
    x: np.ndarray, y: np.ndarray, covariates: np.ndarray, regions: np.ndarray
) -> tuple[float, float]:
    """Slope of y on x with an intercept and covariates, and its region-clustered standard error."""
    design = np.column_stack([np.ones_like(x), covariates, x])
    coefficients = np.linalg.lstsq(design, y, rcond=None)[0]
    residuals = y - design @ coefficients
    bread = np.linalg.inv(design.T @ design)
    scores = [
        design[regions == region].T @ residuals[regions == region]
        for region in np.unique(regions)
    ]
    variance = bread @ sum(np.outer(score, score) for score in scores) @ bread
    return float(coefficients[-1]), float(np.sqrt(variance[-1, -1]))


class TestExpressionSpecificity:
    """Forty protein-coding genes g0-g39 and a lncRNA, with g0-g29 on chromosome 1 and g30-g39 on chromosome 2.

    TSSs are 300 kb apart and gene lengths vary, so TSS density and length differ between genes.
    Panel source1 has tissues UBERON_A, where the disease's genes score high, UBERON_B, with
    random scores, and UBERON_C, where every gene scores 0. Panel source2 has one cell type,
    CL_A, with random scores. disease1 lists g0-g7, the lncRNA and g30-g31.
    """

    n_genes = 40
    rng = np.random.default_rng(1)
    tss = np.array([300_000 * (i % 30) + 1_000 for i in range(40)])
    lengths = rng.integers(1_000, 100_000, 40)
    chromosomes = np.array(["1"] * 30 + ["2"] * 10)
    listed = np.isin(np.arange(40), [*range(8), 30, 31]).astype(float)
    scores = {
        "UBERON_A": listed * 0.8 + rng.random(40) * 0.3,
        "UBERON_B": rng.random(40),
        "UBERON_C": np.zeros(40),
        "CL_A": rng.random(40),
    }

    @pytest.fixture()
    def target_index(self, spark: SparkSession) -> TargetIndex:
        """The forty protein-coding genes and the lncRNA nc1 next to g0."""
        rows = [
            {
                "id": f"g{i}",
                "biotype": "protein_coding",
                "tss": int(self.tss[i]),
                "genomicLocation": {
                    "chromosome": str(self.chromosomes[i]),
                    "start": int(self.tss[i]),
                    "end": int(self.tss[i] + self.lengths[i]),
                },
            }
            for i in range(self.n_genes)
        ]
        rows.append(
            {
                "id": "nc1",
                "biotype": "lncRNA",
                "tss": 2_000,
                "genomicLocation": {"chromosome": "1", "start": 2_000, "end": 3_000},
            }
        )
        return TargetIndex(
            _df=spark.createDataFrame(rows, TargetIndex.get_schema()),
            _schema=TargetIndex.get_schema(),
        )

    @pytest.fixture()
    def expression_specificity(self, spark: SparkSession) -> ExpressionSpecificity:
        """The two panels, plus proteomics rows that carry no specificity score."""
        rows: list[tuple[Any, ...]] = []
        for biosample, values in self.scores.items():
            tissue, celltype, source = (
                (None, biosample, "source2")
                if biosample.startswith("CL")
                else (biosample, None, "source1")
            )
            rows += [
                (f"g{i}", source, tissue, celltype, float(value))
                for i, value in enumerate(values)
            ]
            rows.append(("nc1", source, tissue, celltype, 1.0))
        rows += [(f"g{i}", "proteomics", "UBERON_A", None, None) for i in range(10)]
        return ExpressionSpecificity(
            baseline_expression=spark.createDataFrame(rows, BASELINE_EXPRESSION_SCHEMA),
            panels=PANELS,
        )

    @pytest.fixture()
    def gene_lists(self, spark: SparkSession) -> DataFrame:
        """disease1: g0-g7 and the lncRNA on chromosome 1, g30 and g31 on chromosome 2."""
        genes = [("1", f"g{i}") for i in range(8)] + [("1", "nc1")]
        genes += [("2", "g30"), ("2", "g31")]
        return spark.createDataFrame(
            [("disease1", chromosome, gene) for chromosome, gene in genes],
            "diseaseId string, chromosome string, geneId string",
        )

    def _expected(self, biosample: str, kept: np.ndarray) -> tuple[float, float]:
        """Slope and clustered standard error computed independently over the kept genes."""
        # TSS density counts every biotype, so the lncRNA at 2 kb on chromosome 1 too.
        density = np.array(
            [
                np.sum(
                    (self.chromosomes == self.chromosomes[i])
                    & (np.abs(self.tss - self.tss[i]) <= 1_000_000)
                )
                + int(
                    self.chromosomes[i] == "1" and abs(2_000 - self.tss[i]) <= 1_000_000
                )
                for i in range(self.n_genes)
            ]
        )
        covariates = np.column_stack([np.log10(self.lengths), density])
        regions = np.char.add(
            np.char.add(self.chromosomes, ":"), (self.tss // 1_000_000).astype(str)
        )
        return _clustered_ols(
            self.scores[biosample][kept],
            self.listed[kept],
            covariates[kept],
            regions[kept],
        )

    @staticmethod
    def _results(
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
        **kwargs: Any,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        kwargs = {"p_value_adjusted_threshold": 2.0, "min_genes": 4} | kwargs
        return {
            (row["heldOutChromosome"], row["biosampleId"]): row.asDict()
            for row in expression_specificity.enrichment(
                gene_lists, target_index, **kwargs
            ).collect()
        }

    def test_specificity_reads_each_panel_at_its_resolution(
        self, spark: SparkSession
    ) -> None:
        """A cell type within a tissue is identified by the cell type, with the tissue alongside; duplicates keep the highest score."""
        rows = [
            ("g0", "source1", "UBERON_A", None, 0.5),
            ("g0", "source1", None, "CL_A", 0.6),
            ("g0", "source1", "UBERON_A", "CL_A", 0.7),
            ("g0", "source1", "UBERON_A", "CL_A", 0.9),
            ("g0", "source2", "UBERON_A", None, 0.1),
        ]
        specificity = ExpressionSpecificity(
            baseline_expression=spark.createDataFrame(rows, BASELINE_EXPRESSION_SCHEMA)
        )
        panels = [
            SpecificityPanel("source1", "tissue"),
            SpecificityPanel("source1", "celltype"),
            SpecificityPanel("source1", "tissue_celltype"),
        ]
        read = {
            panel.biosample_type: specificity.specificity(panel).to_dict("records")
            for panel in panels
        }
        assert read == {
            "tissue": [
                {
                    "tissueBiosampleId": None,
                    "biosampleId": "UBERON_A",
                    "geneId": "g0",
                    "specificityScore": 0.5,
                }
            ],
            "celltype": [
                {
                    "tissueBiosampleId": None,
                    "biosampleId": "CL_A",
                    "geneId": "g0",
                    "specificityScore": 0.6,
                }
            ],
            "tissue_celltype": [
                {
                    "tissueBiosampleId": "UBERON_A",
                    "biosampleId": "CL_A",
                    "geneId": "g0",
                    "specificityScore": 0.9,
                }
            ],
        }

    def test_slope_and_error_match_a_clustered_regression(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """Holding out chromosome 2 regresses over g0-g29; the lncRNA is not in the universe."""
        results = self._results(expression_specificity, gene_lists, target_index)
        kept = self.chromosomes != "2"
        for biosample in ("UBERON_A", "UBERON_B", "CL_A"):
            beta, standard_error = self._expected(biosample, kept)
            row = results[("2", biosample)]
            assert row["beta"] == pytest.approx(beta)
            assert row["betaError"] == pytest.approx(standard_error)
            assert row["pValue"] == pytest.approx(norm.sf(beta / standard_error))
        assert results[("2", "UBERON_A")]["beta"] > 0
        assert results[("2", "UBERON_A")]["pValue"] < 0.05

    def test_benjamini_hochberg_runs_over_every_panel(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """UBERON_C has no variance, so three biosamples of two panels share one adjustment."""
        results = self._results(expression_specificity, gene_lists, target_index)
        biosamples = ["CL_A", "UBERON_A", "UBERON_B"]
        assert {key for key in results if key[0] == "2"} == {
            ("2", biosample) for biosample in biosamples
        }
        p_values = np.array([[results[("2", b)]["pValue"] for b in biosamples]])
        np.testing.assert_allclose(
            [results[("2", b)]["pValueAdjusted"] for b in biosamples],
            benjamini_hochberg(p_values)[0],
        )
        assert results[("2", "CL_A")]["datasourceId"] == "source2"

    def test_genes_of_the_held_out_chromosome_are_left_out(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """Holding out chromosome 1 leaves g30 and g31, too few for four required genes."""
        results = self._results(
            expression_specificity, gene_lists, target_index, min_genes=2
        )
        kept = self.chromosomes != "1"
        assert results[("1", "UBERON_B")]["beta"] == pytest.approx(
            self._expected("UBERON_B", kept)[0]
        )
        assert {
            key[0]
            for key in self._results(expression_specificity, gene_lists, target_index)
        } == {"2"}

    def test_only_the_requested_chromosomes_are_held_out(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """Asking for chromosome 2 alone returns no rows for chromosome 1."""
        results = self._results(
            expression_specificity,
            gene_lists,
            target_index,
            min_genes=2,
            held_out_chromosomes=["2"],
        )
        assert {key[0] for key in results} == {"2"}

    def test_only_enriched_biosamples_are_returned(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """At the default threshold only UBERON_A, where the disease's genes score high, is kept."""
        results = self._results(
            expression_specificity,
            gene_lists,
            target_index,
            p_value_adjusted_threshold=0.05,
        )
        assert set(results) == {("2", "UBERON_A")}

    def test_diseases_with_too_few_genes_are_not_tested(
        self,
        expression_specificity: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
    ) -> None:
        """The lncRNA does not count towards the eight protein-coding genes of disease1 on chromosome 1."""
        assert (
            self._results(expression_specificity, gene_lists, target_index, min_genes=9)
            == {}
        )
