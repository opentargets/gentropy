"""Tests for FumaGene2Func hypergeometric tissue enrichment."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import Row, SparkSession

from gentropy.method.fuma_gene2func import FumaGene2Func


class TestGene2FuncErrors:
    """Tests for validation and error paths in gene2func_enrichment."""

    def test_raises_if_study_locus_id_without_credible_set(
        self, spark: SparkSession
    ) -> None:
        """ValueError raised when scored_df has studyLocusId but credible_set_df is None."""
        scored_df = spark.createDataFrame(
            [Row(studyLocusId="SL1", geneId="G1", score=0.9)]
        )
        gene_sets_df = spark.createDataFrame([Row(setName="S1", geneId="G1")])
        with pytest.raises(ValueError, match="credible_set_df"):
            FumaGene2Func.gene2func_enrichment(
                scored_df=scored_df,
                gene_sets_df=gene_sets_df,
                gene_col="geneId",
                score_col="score",
                credible_set_df=None,
            )

    def test_raises_if_no_group_columns(self, spark: SparkSession) -> None:
        """ValueError raised when no group columns remain after stripping gene/score."""
        scored_df = spark.createDataFrame([Row(geneId="G1", score=0.9)])
        gene_sets_df = spark.createDataFrame([Row(setName="S1", geneId="G1")])
        with pytest.raises(ValueError, match="No group columns"):
            FumaGene2Func.gene2func_enrichment(
                scored_df=scored_df,
                gene_sets_df=gene_sets_df,
                gene_col="geneId",
                score_col="score",
            )

    def test_raises_if_gene_col_ambiguous_in_gene_sets(
        self, spark: SparkSession
    ) -> None:
        """ValueError raised when gene_sets_df has multiple non-set columns and none matches gene_col."""
        scored_df = spark.createDataFrame(
            [Row(diseaseId="D1", targetId="G1", score=0.9)]
        )
        # gene_sets_df has two non-set columns — ambiguous
        gene_sets_df = spark.createDataFrame(
            [Row(setName="S1", geneId="G1", extra="x")]
        )
        with pytest.raises(ValueError, match="Cannot resolve the gene column"):
            FumaGene2Func.gene2func_enrichment(
                scored_df=scored_df,
                gene_sets_df=gene_sets_df,
                gene_col="targetId",
                score_col="score",
            )


class TestGene2FuncEnrichmentStatistics:
    """Tests for correctness of enrichment counts and statistics."""

    @pytest.fixture()
    def tiny_enrichment_result(self, spark: SparkSession):  # type: ignore[no-untyped-def]
        """Run gene2func on a tiny, hand-verifiable example and return the result rows.

        Setup:
            - Group: diseaseId="D1"
            - Universe (gene set members): G1..G10
            - Gene sets: SET1={G1..G5}, SET2={G6..G10}
            - scored_df: G1-G5 score 0.9 (above threshold), G6-G10 score 0.1
            - score_threshold=0.4  ->  n_input=5, n_background=10
            - k_gene_set SET1=5, k_gene_set SET2=5
            - k_overlap SET1=5, k_overlap SET2=0

        Expected:
            - p_value SET1 = hypergeom.sf(4, 10, 5, 5)  ~= 0.003968
            - p_value SET2 = 1.0  (k_overlap == 0)
            - fold_enrichment SET1 = 5 / (5*5/10) = 2.0
            - n_tests per group = 2  (both sets tested in D1)
        """
        from scipy.stats import hypergeom

        rows_scored = [
            Row(diseaseId="D1", geneId=f"G{i}", score=0.9) for i in range(1, 6)
        ] + [Row(diseaseId="D1", geneId=f"G{i}", score=0.1) for i in range(6, 11)]
        scored_df = spark.createDataFrame(rows_scored)

        rows_sets = [Row(setName="SET1", geneId=f"G{i}") for i in range(1, 6)] + [
            Row(setName="SET2", geneId=f"G{i}") for i in range(6, 11)
        ]
        gene_sets_df = spark.createDataFrame(rows_sets)

        result = FumaGene2Func.gene2func_enrichment(
            scored_df=scored_df,
            gene_sets_df=gene_sets_df,
            gene_col="geneId",
            score_col="score",
            score_threshold=0.4,
            min_genes=1,
        )

        rows = {r["setName"]: r for r in result.collect()}
        expected_p_set1 = float(hypergeom.sf(4, 10, 5, 5))
        return rows, expected_p_set1

    def test_p_value_set_with_full_overlap(
        self, tiny_enrichment_result: tuple[dict[str, Any], float]
    ) -> None:
        """SET1 p-value matches scipy hypergeom.sf reference."""
        rows, expected_p = tiny_enrichment_result
        assert rows["SET1"]["p_value"] == pytest.approx(expected_p, rel=1e-6)

    def test_p_value_set_with_no_overlap(
        self, tiny_enrichment_result: tuple[dict[str, Any], float]
    ) -> None:
        """SET2 p-value is 1.0 when k_overlap == 0."""
        rows, _ = tiny_enrichment_result
        assert rows["SET2"]["p_value"] == pytest.approx(1.0)

    def test_fold_enrichment(
        self, tiny_enrichment_result: tuple[dict[str, Any], float]
    ) -> None:
        """SET1 fold enrichment is 2.0 (observed=5, expected=2.5)."""
        rows, _ = tiny_enrichment_result
        assert rows["SET1"]["fold_enrichment"] == pytest.approx(2.0)

    def test_bonferroni_uses_per_group_count(
        self, tiny_enrichment_result: tuple[dict[str, Any], float]
    ) -> None:
        """p_bonferroni = min(1, p_value * n_group_tests) where n_group_tests=2."""
        rows, expected_p = tiny_enrichment_result
        expected_bonf = min(1.0, expected_p * 2)
        assert rows["SET1"]["p_bonferroni"] == pytest.approx(expected_bonf, rel=1e-6)

    def test_fdr_bh_ordering(
        self, tiny_enrichment_result: tuple[dict[str, Any], float]
    ) -> None:
        """p_fdr_bh for the most significant set <= p_fdr_bh for the least significant."""
        rows, _ = tiny_enrichment_result
        assert rows["SET1"]["p_fdr_bh"] <= rows["SET2"]["p_fdr_bh"]

    def test_duplicate_gene_set_rows_do_not_inflate_counts(
        self, spark: SparkSession
    ) -> None:
        """Duplicated (setName, geneId) pairs must not change any count or p-value.

        Duplicates inflate K and k via count("*") while N and n stay correct,
        producing impossible configurations (K > N, k > n) that drive p-values
        towards zero. The gene sets are deduplicated so both runs must agree.
        """
        scored_df = spark.createDataFrame(
            [
                Row(diseaseId="D1", geneId=f"G{i}", score=0.9 if i <= 5 else 0.1)
                for i in range(1, 11)
            ]
        )
        clean = spark.createDataFrame(
            [Row(setName="SET1", geneId=f"G{i}") for i in range(1, 6)]
            + [Row(setName="SET2", geneId=f"G{i}") for i in range(6, 11)]
        )
        duplicated = clean.union(clean)

        def run(gene_sets: Any) -> dict[str, Any]:
            result = FumaGene2Func.gene2func_enrichment(
                scored_df=scored_df,
                gene_sets_df=gene_sets,
                gene_col="geneId",
                score_col="score",
                score_threshold=0.5,
                min_genes=1,
            )
            return {r["setName"]: r for r in result.collect()}

        clean_rows, dup_rows = run(clean), run(duplicated)
        for set_name in ("SET1", "SET2"):
            for col in (
                "n_background",
                "k_gene_set",
                "n_input",
                "k_overlap",
                "p_value",
            ):
                assert dup_rows[set_name][col] == pytest.approx(
                    clean_rows[set_name][col]
                ), f"{set_name}.{col} changed when gene sets were duplicated"
        # Guard the specific invariants that duplication used to violate.
        assert clean_rows["SET1"]["k_gene_set"] <= clean_rows["SET1"]["n_background"]
        assert clean_rows["SET1"]["k_overlap"] <= clean_rows["SET1"]["n_input"]

    def test_gene_col_auto_rename(self, spark: SparkSession) -> None:
        """gene_sets_df with a single non-set column is auto-renamed to gene_col."""
        scored_df = spark.createDataFrame(
            [Row(diseaseId="D1", targetId=f"G{i}", score=0.9) for i in range(1, 6)]
            + [Row(diseaseId="D1", targetId=f"G{i}", score=0.1) for i in range(6, 9)]
        )
        # gene_sets_df uses "geneId" instead of "targetId" — should be auto-renamed
        gene_sets_df = spark.createDataFrame(
            [Row(setName="SET1", geneId=f"G{i}") for i in range(1, 9)]
        )
        result = FumaGene2Func.gene2func_enrichment(
            scored_df=scored_df,
            gene_sets_df=gene_sets_df,
            gene_col="targetId",
            score_col="score",
            score_threshold=0.4,
            min_genes=1,
        )
        assert result.count() >= 1


class TestGene2FuncGroupColumns:
    """Tests for explicit and inferred group column selection."""

    @pytest.fixture()
    def l2g_shaped(self, spark: SparkSession):  # type: ignore[no-untyped-def]
        """An L2GPrediction-shaped frame carrying a per-row `features` vector."""
        scored_df = spark.createDataFrame(
            [
                Row(
                    studyLocusId="SL1",
                    geneId=f"G{i}",
                    score=0.9,
                    features=[Row(name="f1", value=float(i))],
                )
                for i in range(1, 6)
            ]
        )
        credible_set_df = spark.createDataFrame([Row(studyLocusId="SL1", studyId="S1")])
        gene_sets_df = spark.createDataFrame(
            [Row(setName="SET1", geneId=f"G{i}") for i in range(1, 6)]
        )
        return scored_df, credible_set_df, gene_sets_df

    def test_raises_if_inferred_group_col_is_non_primitive(
        self, l2g_shaped: tuple[Any, Any, Any]
    ) -> None:
        """An array/struct column left in scored_df must raise, not silently group per row.

        Inferring `features` as a group column makes (studyId, features) a row
        identifier, giving N=K=n=k=1 and p_value=1.0 for every gene, which reads
        as "no enrichment" rather than as malformed input.
        """
        scored_df, credible_set_df, gene_sets_df = l2g_shaped
        with pytest.raises(ValueError, match="non-primitive"):
            FumaGene2Func.gene2func_enrichment(
                scored_df=scored_df,
                gene_sets_df=gene_sets_df,
                gene_col="geneId",
                score_col="score",
                score_threshold=0.5,
                min_genes=1,
                credible_set_df=credible_set_df,
            )

    def test_explicit_group_cols_ignores_extra_columns(
        self, l2g_shaped: tuple[Any, Any, Any]
    ) -> None:
        """Passing group_cols explicitly groups by studyId and drops `features`."""
        scored_df, credible_set_df, gene_sets_df = l2g_shaped
        result = FumaGene2Func.gene2func_enrichment(
            scored_df=scored_df,
            gene_sets_df=gene_sets_df,
            gene_col="geneId",
            score_col="score",
            score_threshold=0.5,
            min_genes=1,
            credible_set_df=credible_set_df,
            group_cols=["studyId"],
        )
        rows = result.collect()
        assert "features" not in result.columns
        assert len(rows) == 1, "all five genes should collapse into one study group"
        assert rows[0]["studyId"] == "S1"
        assert rows[0]["n_background"] == 5
        assert rows[0]["k_overlap"] == 5

    def test_raises_if_explicit_group_col_missing(
        self, l2g_shaped: tuple[Any, Any, Any]
    ) -> None:
        """A group_cols entry absent after resolution raises a clear error."""
        scored_df, credible_set_df, gene_sets_df = l2g_shaped
        with pytest.raises(ValueError, match="not present after identifier"):
            FumaGene2Func.gene2func_enrichment(
                scored_df=scored_df,
                gene_sets_df=gene_sets_df,
                gene_col="geneId",
                score_col="score",
                credible_set_df=credible_set_df,
                group_cols=["nonexistentColumn"],
            )
