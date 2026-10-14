"""Hypergeometric tissue enrichment for GWAS-prioritised genes (FUMA gene2func approach)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, DoubleType, MapType, StructType

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

    Examples:
        Open Targets association scores need no identifier resolution, so the
        output is grouped by `diseaseId` alone. Genes G1-G3 are prioritised and
        make up the whole of SET1, so SET1 is enriched two-fold over the
        six-gene background while SET2 has no prioritised members.

        >>> scored = spark.createDataFrame(
        ...     [("D1", "G1", 0.9), ("D1", "G2", 0.9), ("D1", "G3", 0.9),
        ...      ("D1", "G4", 0.1), ("D1", "G5", 0.1), ("D1", "G6", 0.1)],
        ...     ["diseaseId", "targetId", "score"],
        ... )
        >>> gene_sets = spark.createDataFrame(
        ...     [("SET1", "G1"), ("SET1", "G2"), ("SET1", "G3"),
        ...      ("SET2", "G4"), ("SET2", "G5"), ("SET2", "G6")],
        ...     ["setName", "targetId"],
        ... )
        >>> FumaGene2Func.gene2func_enrichment(
        ...     scored_df=scored,
        ...     gene_sets_df=gene_sets,
        ...     gene_col="targetId",
        ...     score_col="score",
        ...     score_threshold=0.5,
        ...     min_genes=1,
        ... ).selectExpr(
        ...     "setName", "n_input", "k_overlap", "fold_enrichment",
        ...     "round(p_value, 4) as p_value", "round(p_fdr_bh, 4) as p_fdr_bh",
        ... ).show()
        +-------+-------+---------+---------------+-------+--------+
        |setName|n_input|k_overlap|fold_enrichment|p_value|p_fdr_bh|
        +-------+-------+---------+---------------+-------+--------+
        |   SET1|      3|        3|            2.0|   0.05|     0.1|
        |   SET2|      3|        0|            0.0|    1.0|     1.0|
        +-------+-------+---------+---------------+-------+--------+
        <BLANKLINE>

        L2G predictions are keyed by `studyLocusId`, so a credible set DataFrame
        is required to resolve them. Both loci below belong to study S1, so they
        collapse into a single study-level group.

        >>> l2g = spark.createDataFrame(
        ...     [("SL1", "G1", 0.9), ("SL1", "G2", 0.9), ("SL2", "G3", 0.9),
        ...      ("SL2", "G4", 0.1), ("SL2", "G5", 0.1), ("SL2", "G6", 0.1)],
        ...     ["studyLocusId", "geneId", "score"],
        ... )
        >>> credible_sets = spark.createDataFrame(
        ...     [("SL1", "S1"), ("SL2", "S1")], ["studyLocusId", "studyId"],
        ... )
        >>> sets = spark.createDataFrame(
        ...     [("SET1", "G1"), ("SET1", "G2"), ("SET1", "G3"),
        ...      ("SET2", "G4"), ("SET2", "G5"), ("SET2", "G6")],
        ...     ["setName", "geneId"],
        ... )
        >>> FumaGene2Func.gene2func_enrichment(
        ...     scored_df=l2g,
        ...     gene_sets_df=sets,
        ...     gene_col="geneId",
        ...     score_col="score",
        ...     score_threshold=0.5,
        ...     min_genes=1,
        ...     credible_set_df=credible_sets,
        ... ).selectExpr("studyId", "setName", "n_input", "k_overlap").show()
        +-------+-------+-------+---------+
        |studyId|setName|n_input|k_overlap|
        +-------+-------+-------+---------+
        |     S1|   SET1|      3|        3|
        |     S1|   SET2|      3|        0|
        +-------+-------+-------+---------+
        <BLANKLINE>
    """

    @staticmethod
    def locus_window_background(
        credible_set_df: DataFrame,
        target_index_df: DataFrame,
        half_window: int = 500_000,
        group_cols: list[str] | None = None,
    ) -> DataFrame:
        """Build a background of every gene in contention around a group's loci.

        Produces the `background_df` that `gene2func_enrichment` accepts: one row per
        (group, gene) for every gene whose TSS lies within `half_window` of a credible
        set's lead variant.

        This exists because the default background -- the genes the group scored -- is
        a model output. For L2G input it means "genes L2G chose to score", so any bias
        in that choice becomes part of the null. The genes that were ever *candidates*
        are a property of the genome and the loci instead, and `half_window` defaults
        to 500kb because that is L2G's own reach: its distance feature scores a gene
        `genomic_window - |distance| + 1` with `genomic_window = 500_000`, so anything
        further away scores non-positive and is not really in contention.

        Note this is anchored on the lead variant `position`, not on
        `locusStart`/`locusEnd`. Those are null for 83% of credible sets in a release
        and carry different meanings across fine-mapping methods, so a background
        built from them is silently empty for most studies.

        Args:
            credible_set_df (DataFrame): Credible sets, with the group columns plus
                `chromosome` and `position`.
            target_index_df (DataFrame): Gene index, with `geneId` (or `id`),
                `chromosome` and `tss`.
            half_window (int): Half-width of the window in base pairs.
            group_cols (list[str] | None): Grouping columns, defaulting to
                `["studyId"]`.

        Returns:
            DataFrame: Distinct rows of the group columns plus `geneId`.
        """
        group_cols = group_cols or ["studyId"]
        genes = target_index_df
        if "geneId" not in genes.columns:
            genes = genes.withColumnRenamed("id", "geneId")
        genes = genes.select("geneId", "chromosome", "tss").filter(
            f.col("tss").isNotNull() & f.col("chromosome").isNotNull()
        )

        loci = (
            credible_set_df.select(*group_cols, "chromosome", "position")
            .filter(f.col("position").isNotNull())
            .distinct()
        )
        # The gene index is small enough to broadcast, which keeps the range join
        # from degenerating into a shuffle over every (locus, gene) pair.
        return (
            loci.join(
                f.broadcast(genes.withColumnRenamed("chromosome", "_gchrom")),
                on=(
                    (f.col("chromosome") == f.col("_gchrom"))
                    & (f.col("tss") >= f.col("position") - half_window)
                    & (f.col("tss") <= f.col("position") + half_window)
                ),
                how="inner",
            )
            .select(*group_cols, "geneId")
            .distinct()
        )

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
        background_df: DataFrame | None = None,
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
            background_df (DataFrame | None): Optional background, one row per
                (group combination, gene). When omitted the background is the set of
                scored genes, which conditions the test on what the group scored.

        Returns:
            DataFrame: One row per (group combination x gene set) containing
                enrichment counts and statistics.
        """
        # Reduce gene_sets_df to exactly (set_col, gene_col), deduplicated.
        # If gene_col is already present, select it directly.
        # Otherwise look for a single non-set column and alias it as gene_col.
        # Any extra metadata columns are intentionally dropped here.
        #
        # The distinct() is load-bearing: gene_sets_df carries no uniqueness
        # guarantee, and repeated (set_col, gene_col) pairs are common in
        # practice (per-subregion GTEx rows, several transcripts per gene, a
        # union of up- and down-regulated tables). Because the counting joins
        # below aggregate with count("*"), duplicates would inflate K and k
        # while N and n stay correct -- they come from the already-collapsed
        # `scored` side -- yielding impossible configurations such as K > N
        # and p-values driven towards zero.
        if gene_col in gene_sets_df.columns:
            gene_sets_df = gene_sets_df.select(set_col, gene_col).distinct()
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
            ).distinct()

        # Gene set universe: restrict background to genes in any set
        universe = gene_sets_df.select(gene_col).distinct()

        # Background genes per group (N). By default these are the genes the group
        # itself scored, which conditions the test on what was actually interrogated.
        # That is defensible but it conditions on a MODEL OUTPUT: for L2G input, the
        # background becomes "genes L2G chose to score", so any bias in that choice
        # is baked into the null. A caller can instead supply the genes that were
        # ever in contention -- for L2G, every gene within its +/-500kb candidate
        # window -- which is a property of the genome rather than of the model.
        background_genes = (
            scored.select(*group_cols, gene_col)
            if background_df is None
            else background_df.select(*group_cols, gene_col).distinct()
        ).join(universe, on=gene_col, how="inner")

        background = background_genes.groupBy(*group_cols).agg(
            f.count("*").alias("n_background")
        )

        # Input genes: above threshold, and restricted to the background (n). The
        # intersection is load-bearing for a supplied background -- a scored gene
        # need not fall inside it, and n > N or k > K would be an invalid
        # hypergeometric configuration.
        input_genes_all = (
            scored.filter(f.col(score_col) >= score_threshold)
            .select(*group_cols, gene_col)
            .join(background_genes, on=[*group_cols, gene_col], how="left_semi")
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
            background_genes.join(qualifying_groups, on=group_cols, how="inner")
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

            Raises:
                ValueError: If the counts are not a valid hypergeometric
                    configuration, which would otherwise yield a silently
                    wrong p-value rather than an error.
            """
            from scipy.stats import hypergeom  # noqa: PLC0415

            if K > N or n > N or k > K or k > n:
                raise ValueError(
                    f"Invalid hypergeometric configuration: k={k}, K={K}, "
                    f"n={n}, N={N}. Expected k <= min(K, n) and max(K, n) <= N. "
                    "This usually means gene_sets_df contained duplicate "
                    f"('{set_col}', '{gene_col}') pairs."
                )
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
        group_cols: list[str] | None = None,
        background_df: DataFrame | None = None,
    ) -> DataFrame:
        """Run tissue enrichment from any scored gene DataFrame.

        Resolves identifiers in order, then groups by group_cols, falling back
        to inferring them as all columns remaining after removing gene_col and
        score_col. The output columns and grouping therefore reflect exactly
        what was resolvable from the inputs.

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
            group_cols (list[str] | None): Optional explicit list of columns to
                group the enrichment by, applied after identifier resolution.
                When omitted, the group columns are inferred as every column
                left after removing gene_col and score_col. Pass this
                explicitly when scored_df carries non-identifier columns, such
                as the `features` vector on L2GPrediction.df.
            background_df (DataFrame | None): Optional background: the genes that were
                ever in contention per group, with the same group columns as the
                output plus gene_col. Defaults to the scored genes.

                !!! tip "A wider background is usually better"
                    The default conditions on a model output -- for L2G input it is
                    "genes L2G chose to score" -- and gives a small population
                    (median 47 genes), so p-values are coarse and a fifth of rows have
                    fewer than 20 background genes. Supplying the genes within L2G's
                    +/-500kb candidate window instead raised mean AUROC against a
                    curated trait-to-tissue gold standard at all 16 L2G thresholds
                    tested (+0.03 typical, 0.692 to 0.720 at a 0.5 cutoff), and
                    retained 98.5% of the default's significant hits. It also avoids
                    degenerating at low score thresholds, where the default collapses
                    to AUROC 0.500 because the input set becomes the background.

        Returns:
            DataFrame: One row per (resolved group combination x gene set).
                Group columns come first, followed by the gene set label,
                enrichment counts (n_background, k_gene_set, n_input, k_overlap),
                and statistics (expected_overlap, fold_enrichment, p_value,
                p_bonferroni, p_fdr_bh).

        Raises:
            ValueError: If scored_df contains studyLocusId but no
                credible_set_df is provided.
            ValueError: If an explicit group_cols entry is not present after
                identifier resolution.
            ValueError: If an inferred group column has a non-primitive type,
                which can never be a sensible enrichment grouping key.
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

        # 3. Resolve group columns, either explicit or inferred
        if group_cols is not None:
            missing = [c for c in group_cols if c not in working.columns]
            if missing:
                raise ValueError(
                    f"group_cols {missing} are not present after identifier "
                    f"resolution. Columns available: {working.columns}."
                )
        else:
            non_group = {gene_col, score_col}
            group_cols = [c for c in working.columns if c not in non_group]

            # Inference is a convenience, not a contract: a per-row column left
            # in scored_df would silently become part of the enrichment key and
            # split every gene into its own group, yielding N=K=n=k=1 and a
            # p-value of exactly 1.0 for every row. That reads as "no
            # enrichment" rather than as malformed input, so reject the
            # non-primitive types that can never be a sensible grouping key
            # and make the caller pass group_cols explicitly instead.
            non_primitive = [
                c
                for c in group_cols
                if isinstance(
                    working.schema[c].dataType, (ArrayType, MapType, StructType)
                )
            ]
            if non_primitive:
                raise ValueError(
                    f"Inferred group columns {non_primitive} have non-primitive "
                    "types and cannot be used as enrichment grouping keys. Pass "
                    "group_cols explicitly to select the identifier columns to "
                    f"group by. Columns available: {working.columns}."
                )

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
            background_df=background_df,
        )
