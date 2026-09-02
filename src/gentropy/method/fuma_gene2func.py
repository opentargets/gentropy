"""Hypergeometric tissue enrichment for GWAS-prioritised genes (FUMA gene2func approach)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FumaGene2Func:
    """Hypergeometric gene-set enrichment for GWAS-prioritised genes.

    Implements the FUMA gene2func enrichment approach: for each group (study,
    disease, or study x disease), tests whether the prioritised genes are
    over-represented in tissue-specific gene sets (e.g. GTEx DEGs) relative to
    a per-group background universe.

    The method resolves identifiers automatically and the output contains
    whatever group columns were available after resolution:

    - studyLocusId / geneId / score + credible_set_df
      -> output grouped by studyId
    - studyLocusId / geneId / score + credible_set_df + study_index_df
      -> output grouped by studyId, diseaseId
    - diseaseId / targetId / score (OT association scores, no extras)
      -> output grouped by diseaseId
    - Any other scored DataFrame -- group columns are inferred automatically
      as everything that is not gene_col or score_col.

    Gene sets are supplied as a long-format Spark DataFrame with one row per
    (set label, gene).  GTEx DEG sets, MSigDB gene sets, or any other collection
    can be passed in this format.

    Example::

        # L2G predictions, study-level output
        results = FumaGene2Func.gene2func_enrichment(
            scored_df=l2g_predictions,      # studyLocusId, geneId, score
            gene_sets_df=gtex_deg,          # setName, geneId
            gene_col="geneId",
            score_col="score",
            credible_set_df=cs.select("studyLocusId", "studyId"),
        )
        # -> (studyId, setName, n_background, ..., p_fdr_bh)

        # L2G predictions, study x disease output
        results = FumaGene2Func.gene2func_enrichment(
            scored_df=l2g_predictions,
            gene_sets_df=gtex_deg,
            gene_col="geneId",
            score_col="score",
            credible_set_df=cs.select("studyLocusId", "studyId"),
            study_index_df=study_index.df,
        )
        # -> (studyId, diseaseId, setName, n_background, ..., p_fdr_bh)

        # OT association scores, disease-level output
        results = FumaGene2Func.gene2func_enrichment(
            scored_df=ot_assoc.select("diseaseId", "targetId", "score"),
            gene_sets_df=gtex_deg,
            gene_col="targetId",
            score_col="score",
        )
        # -> (diseaseId, setName, n_background, ..., p_fdr_bh)
    """

    @staticmethod
    def _compute_enrichment(
        scored: DataFrame,
        gene_sets_df: DataFrame,
        group_cols: list[str],
        gene_col: str,
        score_col: str,
        score_threshold: float,
        set_col: str,
        min_genes: int,
    ) -> DataFrame:
        """Core hypergeometric enrichment over arbitrary group columns.

        Args:
            scored (DataFrame): Max-collapsed DataFrame with group_cols, gene_col,
                and score_col -- one row per (group combination, gene).
            gene_sets_df (DataFrame): Long-format gene sets with set_col and a gene column.
                The gene column may be named differently from gene_col (e.g. "geneId" when
                gene_col is "targetId") and will be renamed automatically.
            group_cols (list[str]): List of columns that define the enrichment groups.
            gene_col (str): Gene identifier column.
            score_col (str): Prioritisation score column.
            score_threshold (float): Minimum score to treat a gene as prioritised.
            set_col (str): Gene set label column in gene_sets_df.
            min_genes (int): Minimum prioritised genes required per group.

        Returns:
            DataFrame: One row per (group combination x gene set) containing
                enrichment counts and statistics.
        """
        # Reduce gene_sets_df to exactly (set_col, gene_col).
        # If gene_col is already present, select it directly.
        # Otherwise look for a single non-set column and alias it as gene_col.
        # Any extra metadata columns are intentionally dropped here.
        if gene_col in gene_sets_df.columns:
            gene_sets_df = gene_sets_df.select(set_col, gene_col)
        else:
            candidate_cols = [c for c in gene_sets_df.columns if c != set_col]
            if len(candidate_cols) != 1:
                raise ValueError(
                    f"Cannot resolve the gene column in gene_sets_df. "
                    f"Expected a column named '{gene_col}', or exactly one non-set column "
                    f"that can be renamed automatically, but found columns: "
                    f"{gene_sets_df.columns}. Rename the gene column to '{gene_col}' "
                    f"before passing gene_sets_df."
                )
            gene_sets_df = gene_sets_df.select(
                set_col, f.col(candidate_cols[0]).alias(gene_col)
            )

        # Gene set universe: restrict background to genes in any set
        universe = gene_sets_df.select(gene_col).distinct()

        # Background per group: scored genes intersect universe (N)
        background = (
            scored.select(*group_cols, gene_col)
            .join(universe, on=gene_col, how="inner")
            .groupBy(*group_cols)
            .agg(f.count("*").alias("n_background"))
        )

        # Input genes: above threshold, in universe (n)
        input_genes_all = (
            scored.filter(f.col(score_col) >= score_threshold)
            .select(*group_cols, gene_col)
            .join(universe, on=gene_col, how="inner")
        )
        input_counts = (
            input_genes_all.groupBy(*group_cols)
            .agg(f.count("*").alias("n_input"))
            .filter(f.col("n_input") >= min_genes)
        )
        qualifying_groups = input_counts.select(*group_cols)
        input_genes = input_genes_all.join(
            qualifying_groups, on=group_cols, how="inner"
        )

        # K per (group, set): gene set members present in background
        k_gene_set = (
            scored.select(*group_cols, gene_col)
            .join(universe, on=gene_col, how="inner")
            .join(qualifying_groups, on=group_cols, how="inner")
            .join(gene_sets_df.select(set_col, gene_col), on=gene_col, how="inner")
            .groupBy(*group_cols, set_col)
            .agg(f.count("*").alias("k_gene_set"))
        )

        # k per (group, set): overlap of input genes with gene set
        k_overlap = (
            input_genes.join(
                gene_sets_df.select(set_col, gene_col), on=gene_col, how="inner"
            )
            .groupBy(*group_cols, set_col)
            .agg(f.count("*").alias("k_overlap"))
        )

        # Assemble: (group, set, N, K, n, k)
        counts = (
            k_gene_set.join(k_overlap, on=[*group_cols, set_col], how="left")
            .fillna(0, subset=["k_overlap"])
            .join(background, on=group_cols, how="inner")
            .join(input_counts, on=group_cols, how="inner")
        )

        # Hypergeometric p-value P(X >= k)
        @f.udf(returnType=DoubleType())
        def _hypergeom_sf(k: int, N: int, K: int, n: int) -> float:
            """Return one-sided hypergeometric survival P(X >= k).

            Args:
                k (int): Observed overlap count.
                N (int): Background population size.
                K (int): Gene set members in background.
                n (int): Input gene count.

            Returns:
                float: P(X >= k) under the hypergeometric distribution,
                    or 1.0 if any count is zero.
            """
            from scipy.stats import hypergeom  # noqa: PLC0415

            if k == 0 or n == 0 or K == 0 or N == 0:
                return 1.0
            return float(hypergeom.sf(int(k) - 1, int(N), int(K), int(n)))

        # Windows for per-group BH FDR with proper step-down monotonicity.
        # n_tests is computed per group (number of tested sets in that group),
        # not globally, so corrections are calibrated to the actual family size.
        w_group = Window.partitionBy(*group_cols)
        w_asc = Window.partitionBy(*group_cols).orderBy(f.col("p_value").asc())
        w_desc_cummin = (
            Window.partitionBy(*group_cols)
            .orderBy(f.col("_rank").desc())
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        return (
            counts.withColumn(
                "expected_overlap",
                f.col("n_input").cast(DoubleType())
                * f.col("k_gene_set").cast(DoubleType())
                / f.col("n_background").cast(DoubleType()),
            )
            .withColumn(
                "fold_enrichment",
                f.when(
                    f.col("expected_overlap") > 0,
                    f.col("k_overlap").cast(DoubleType()) / f.col("expected_overlap"),
                ).otherwise(f.lit(0.0).cast(DoubleType())),
            )
            .withColumn(
                "p_value",
                _hypergeom_sf(
                    f.col("k_overlap").cast("int"),
                    f.col("n_background").cast("int"),
                    f.col("k_gene_set").cast("int"),
                    f.col("n_input").cast("int"),
                ),
            )
            .withColumn("_n_tests", f.count("*").over(w_group).cast(DoubleType()))
            .withColumn(
                "p_bonferroni",
                f.least(f.lit(1.0), f.col("p_value") * f.col("_n_tests")),
            )
            .withColumn("_rank", f.rank().over(w_asc))
            .withColumn(
                "_bh_raw",
                f.least(
                    f.lit(1.0),
                    f.col("p_value")
                    * f.col("_n_tests")
                    / f.col("_rank").cast(DoubleType()),
                ),
            )
            .withColumn("p_fdr_bh", f.min("_bh_raw").over(w_desc_cummin))
            .drop("_rank", "_bh_raw", "_n_tests")
            .orderBy(*group_cols, "p_value")
        )

    @staticmethod
    def gene2func_enrichment(
        scored_df: DataFrame,
        gene_sets_df: DataFrame,
        gene_col: str,
        score_col: str,
        score_threshold: float = 0.5,
        set_col: str = "setName",
        min_genes: int = 5,
        credible_set_df: DataFrame | None = None,
        study_index_df: DataFrame | None = None,
        study_disease_col: str = "diseaseIds",
    ) -> DataFrame:
        """Run tissue enrichment from any scored gene DataFrame.

        Resolves identifiers in order, then infers group columns automatically
        as all columns remaining after removing gene_col and score_col.
        The output columns and grouping therefore reflect exactly what was
        resolvable from the inputs.

        Args:
            scored_df (DataFrame): DataFrame containing at minimum gene_col and
                score_col. May additionally contain any combination of
                studyLocusId, studyId, diseaseId, or other
                identifier columns that should appear in the output.
            gene_sets_df (DataFrame): Long-format gene sets DataFrame with columns
                set_col and gene_col.
            gene_col (str): Name of the gene identifier column (e.g. "geneId"
                or "targetId"). Must match the column name used in
                gene_sets_df.
            score_col (str): Name of the prioritisation score column (e.g.
                "score" or "associationScore").
            score_threshold (float): Genes with score >= score_threshold are
                treated as prioritised input genes. Default 0.5.
            set_col (str): Column name for the gene set label in gene_sets_df.
                Default "setName".
            min_genes (int): Minimum number of prioritised genes per group required
                to run enrichment. Groups with fewer genes are dropped.
                Default 5.
            credible_set_df (DataFrame | None): Optional. If scored_df contains
                studyLocusId, this DataFrame must be provided to resolve
                locus identifiers to studyId. Only studyLocusId and
                studyId columns are used.
            study_index_df (DataFrame | None): Optional. If provided and studyId
                is present after resolution, this DataFrame is joined to add disease
                information (study_disease_col) to the grouping, producing
                one enrichment result per (study, disease) pair.
            study_disease_col (str): Column in study_index_df that contains the
                disease identifier(s). May be a scalar string column or an
                array (which will be exploded automatically). Default
                "diseaseIds".

        Returns:
            DataFrame: One row per (resolved group combination x gene set).
                Group columns come first, followed by the gene set label,
                enrichment counts (n_background, k_gene_set, n_input, k_overlap),
                and statistics (expected_overlap, fold_enrichment, p_value,
                p_bonferroni, p_fdr_bh).

        Raises:
            ValueError: If scored_df contains studyLocusId but no
                credible_set_df is provided.
            ValueError: If no group columns can be identified after resolution.
        """
        working = scored_df

        # 1. Resolve studyLocusId -> studyId
        if "studyLocusId" in working.columns:
            if credible_set_df is None:
                raise ValueError(
                    "scored_df contains 'studyLocusId' but credible_set_df was not "
                    "provided to resolve it to studyId."
                )
            working = working.join(
                credible_set_df.select("studyLocusId", "studyId"),
                on="studyLocusId",
                how="inner",
            ).drop("studyLocusId")

        # 2. Optionally add diseaseId from study index
        if study_index_df is not None and "studyId" in working.columns:
            disease_mapping = study_index_df.select("studyId", study_disease_col)
            if dict(disease_mapping.dtypes)[study_disease_col].startswith("array"):
                disease_mapping = disease_mapping.select(
                    "studyId",
                    f.explode(f.col(study_disease_col)).alias("diseaseId"),
                )
            else:
                disease_mapping = disease_mapping.withColumnRenamed(
                    study_disease_col, "diseaseId"
                )
            working = working.join(disease_mapping, on="studyId", how="inner")

        # 3. Infer group columns
        non_group = {gene_col, score_col}
        group_cols = [c for c in working.columns if c not in non_group]

        if not group_cols:
            raise ValueError(
                f"No group columns found after resolution. Columns present: "
                f"{working.columns}. Ensure the DataFrame contains at least one "
                f"identifier column beyond '{gene_col}' and '{score_col}'."
            )

        # 4. Collapse to group level (max score per group x gene)
        scored = working.groupBy(*group_cols, gene_col).agg(
            f.max(f.col(score_col)).alias(score_col)
        )

        return FumaGene2Func._compute_enrichment(
            scored=scored,
            gene_sets_df=gene_sets_df,
            group_cols=group_cols,
            gene_col=gene_col,
            score_col=score_col,
            score_threshold=score_threshold,
            set_col=set_col,
            min_genes=min_genes,
        )
