"""Build cell-type SNP annotations and annotation-stratified LD scores.

This module recreates the "make_annot" and per-annotation LD-score
computation stages of the CELLECT / S-LDSC (``--h2-cts``) workflow using pure
PySpark :class:`~pyspark.sql.DataFrame` transformations.

The steps implemented here are:

1. Reshape a wide *specificity* matrix (genes x cell types, continuous
   expression-specificity values) into a long ``(geneId, annotation, esValue)``
   table (:func:`melt_specificity_matrix`).
2. Map genes to nearby variants using a symmetric genomic window
   (:func:`map_genes_to_variants`).
3. Turn the gene-level specificity values into per-variant annotation values,
   adding an "all genes" control annotation (:func:`build_snp_annotations`).
4. Explode a :class:`~gentropy.dataset.ld_index.LDIndex` into a population
   specific edge list (:func:`explode_ld_index`).
5. Compute annotation-stratified LD scores
   ``l(j, a) = sum_k r(j, k)^2 * a(k)`` and the per-annotation ``M`` totals
   (:func:`compute_annotation_ld_scores`).

The functions are deliberately decoupled from the gentropy ``Step`` machinery so
they can be unit-tested in isolation and reused by orchestration steps.

Approximations relative to the reference S-LDSC / CELLECT implementation:

* No ``r^2`` finite-sample bias correction is applied; the raw pairwise ``r``
  values from the gnomAD LD index are used directly.
* The LD window is inherited from the gnomAD LD index that produced the
  :class:`~gentropy.dataset.ld_index.LDIndex` rather than a fixed 1 cM window.
* ``M`` is defined as the sum of annotation values across genes (i.e. the total
  annotation weight), not the ``M_5_50`` common-variant count used by the
  canonical S-LDSC baseline.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Name of the control annotation covering every gene in the specificity matrix.
CONTROL_ANNOTATION = "all_genes_control"


def melt_specificity_matrix(
    es_matrix: DataFrame,
    gene_id_column: str = "gene",
    strip_gene_version: bool = True,
) -> DataFrame:
    """Reshape a wide expression-specificity matrix into long format.

    Args:
        es_matrix (DataFrame): Wide specificity matrix with one identifier column
            (``gene_id_column``) and one numeric column per cell-type annotation.
            Values are expression-specificity scores that are expected to be
            non-negative.
        gene_id_column (str): Name of the column holding the (Ensembl) gene
            identifier. Defaults to ``"gene"``.
        strip_gene_version (bool): If ``True``, remove a trailing Ensembl version
            suffix (e.g. ``ENSG00000123.4`` -> ``ENSG00000123``) so gene ids match
            the unversioned identifiers used by the gentropy target index.
            Defaults to ``True``.

    Returns:
        DataFrame: Long table with columns ``geneId``, ``annotation`` and
        ``esValue``. Only rows with a strictly positive ``esValue`` are retained.

    Raises:
        ValueError: If ``gene_id_column`` is not present in ``es_matrix`` or if no
            annotation columns remain after removing the identifier column.
    """
    if gene_id_column not in es_matrix.columns:
        raise ValueError(
            f"Gene id column '{gene_id_column}' not found in specificity matrix "
            f"columns: {es_matrix.columns}"
        )
    annotation_columns = [c for c in es_matrix.columns if c != gene_id_column]
    if not annotation_columns:
        raise ValueError(
            "Specificity matrix must contain at least one annotation column "
            "besides the gene id column."
        )
    gene_id = f.col(gene_id_column)
    if strip_gene_version:
        gene_id = f.regexp_replace(gene_id, r"\.\d+$", "")
    long_df = es_matrix.unpivot(
        ids=[gene_id_column],
        values=annotation_columns,
        variableColumnName="annotation",
        valueColumnName="esValue",
    ).select(
        gene_id.alias("geneId"),
        f.col("annotation"),
        f.col("esValue").cast("double").alias("esValue"),
    )
    return long_df.filter(f.col("esValue") > 0.0)


def map_genes_to_variants(
    gene_locations: DataFrame,
    variant_positions: DataFrame,
    window_kb: int = 100,
) -> DataFrame:
    """Map genes to variants that fall within a symmetric genomic window.

    Args:
        gene_locations (DataFrame): Gene coordinates with columns ``geneId``,
            ``chromosome``, ``start`` and ``end`` (1-based positions).
        variant_positions (DataFrame): Variant coordinates with columns
            ``variantId``, ``chromosome`` and ``position``.
        window_kb (int): Half-window (in kilobases) added on both sides of each
            gene body when assigning variants to genes. Defaults to ``100``.

    Returns:
        DataFrame: Table with columns ``variantId`` and ``geneId`` for every
        variant-gene pair inside the window. A variant may map to several genes.
    """
    window_bp = int(window_kb) * 1000
    windowed_genes = gene_locations.select(
        f.col("geneId"),
        f.col("chromosome").alias("geneChromosome"),
        f.greatest(f.col("start") - f.lit(window_bp), f.lit(1)).alias("windowStart"),
        (f.col("end") + f.lit(window_bp)).alias("windowEnd"),
    )
    return (
        variant_positions.join(
            f.broadcast(windowed_genes),
            on=[
                variant_positions["chromosome"] == windowed_genes["geneChromosome"],
                variant_positions["position"] >= windowed_genes["windowStart"],
                variant_positions["position"] <= windowed_genes["windowEnd"],
            ],
            how="inner",
        )
        .select("variantId", "geneId")
        .distinct()
    )


def build_snp_annotations(
    gene_variant_map: DataFrame,
    specificity_long: DataFrame,
    control_annotation: str = CONTROL_ANNOTATION,
) -> DataFrame:
    """Turn gene-level specificity into per-variant annotation values.

    A variant inherits, for each cell-type annotation, the maximum specificity
    value across all genes it maps to. Every variant that maps to at least one
    gene additionally receives the control annotation with value ``1.0``.

    Args:
        gene_variant_map (DataFrame): Variant-gene pairs with columns
            ``variantId`` and ``geneId`` (see :func:`map_genes_to_variants`).
        specificity_long (DataFrame): Long specificity table with columns
            ``geneId``, ``annotation`` and ``esValue`` (see
            :func:`melt_specificity_matrix`).
        control_annotation (str): Name of the control annotation. Defaults to
            :data:`CONTROL_ANNOTATION`.

    Returns:
        DataFrame: Long annotation table with columns ``variantId``,
        ``annotation`` and ``annotationValue``.
    """
    cell_type_annotations = (
        gene_variant_map.join(specificity_long, on="geneId", how="inner")
        .groupBy("variantId", "annotation")
        .agg(f.max("esValue").alias("annotationValue"))
    )
    control = (
        gene_variant_map.select("variantId")
        .distinct()
        .withColumn("annotation", f.lit(control_annotation))
        .withColumn("annotationValue", f.lit(1.0))
    )
    return cell_type_annotations.unionByName(control)


def explode_ld_index(ld_index_df: DataFrame, population: str) -> DataFrame:
    """Explode an LD index into a population-specific pairwise edge list.

    Args:
        ld_index_df (DataFrame): LD index with columns ``variantId`` and
            ``ldSet`` (array of structs ``{tagVariantId, rValues[]{population, r}}``).
        population (str): LD reference population label to select from
            ``rValues`` (e.g. ``"nfe"``).

    Returns:
        DataFrame: Edge list with columns ``variantId``, ``tagVariantId`` and
        ``r`` (the correlation for the requested population).
    """
    return (
        ld_index_df.select(
            f.col("variantId"),
            f.explode("ldSet").alias("ld"),
        )
        .select(
            f.col("variantId"),
            f.col("ld.tagVariantId").alias("tagVariantId"),
            f.explode("ld.rValues").alias("rValue"),
        )
        .filter(f.col("rValue.population") == population)
        .select(
            f.col("variantId"),
            f.col("tagVariantId"),
            f.col("rValue.r").alias("r"),
        )
    )


def compute_annotation_ld_scores(
    annotations_long: DataFrame,
    ld_edges: DataFrame,
    score_variants: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Compute annotation-stratified LD scores and per-annotation ``M`` totals.

    The LD score of variant ``j`` for annotation ``a`` is
    ``l(j, a) = sum_k r(j, k)^2 * a(k)``, where the sum runs over ``j`` itself
    (self term ``r = 1``) and every LD partner ``k``. Because tag variants with a
    zero annotation value contribute nothing, only the non-zero annotation rows
    need to be joined.

    Args:
        annotations_long (DataFrame): Per-variant annotation values with columns
            ``variantId``, ``annotation`` and ``annotationValue`` (see
            :func:`build_snp_annotations`).
        ld_edges (DataFrame): Pairwise LD edge list with columns ``variantId``,
            ``tagVariantId`` and ``r`` (see :func:`explode_ld_index`). Edges are
            assumed to be directed; they are symmetrised internally.
        score_variants (DataFrame | None): Optional single-column dataframe of
            ``variantId`` values to retain as scored/index variants ``j``. Tag
            variants ``k`` are never restricted by this filter, so contributions
            from the complete edge reference are preserved. M values are always
            computed from the complete ``annotations_long`` dataframe.

    Returns:
        tuple[DataFrame, DataFrame]: A pair ``(ld_scores_long, m_annot)`` where
        ``ld_scores_long`` has columns ``variantId``, ``annotation`` and
        ``ldScore``, and ``m_annot`` has columns ``annotation`` and ``M``.
    """
    # Flat exports contain one directed row for each index/tag pair.  Restore
    # the reverse row for every scored endpoint, including scored variants that
    # also occur on the tag side of another row.  Restricting the union to the
    # scored endpoint keeps the work bounded by the output SNP universe while
    # retaining all tag contributions.  The production exporter emits an
    # upper-triangle edge set, so no global edge deduplication is needed here.
    directed = ld_edges.filter(f.col("variantId") != f.col("tagVariantId"))
    if score_variants is not None:
        score_ids = score_variants.select("variantId").distinct()
        forward = directed.join(score_ids, on="variantId", how="inner")
        reverse = (
            directed.join(
                score_ids.withColumnRenamed("variantId", "tagVariantId"),
                on="tagVariantId",
                how="inner",
            )
            .select(
                f.col("tagVariantId").alias("variantId"),
                f.col("variantId").alias("tagVariantId"),
                f.col("r"),
            )
        )
        candidate_edges = forward.unionByName(reverse)
    else:
        candidate_edges = directed.unionByName(
            directed.select(
                f.col("tagVariantId").alias("variantId"),
                f.col("variantId").alias("tagVariantId"),
                f.col("r"),
            )
        )
    # Zero-correlation rows cannot contribute to any annotation LD score.  The
    # generated min-r2=0 exports contain a large number of these rows, so this
    # predicate is a substantial memory reduction and is mathematically exact.
    candidate_edges = candidate_edges.filter(f.col("r") != 0.0)

    # Push the annotation-tag restriction into the edge read.  It is equivalent
    # to applying the join below, but avoids shuffling/storing unannotated LD
    # partners (the dominant cost for the complete 1-cM edge sets).
    annotation_tags = annotations_long.select("variantId").distinct()
    candidate_edges = candidate_edges.join(
        annotation_tags.withColumnRenamed("variantId", "tagVariantId"),
        on="tagVariantId",
        how="inner",
    )

    # Self term (r = 1) for every scored/reference variant that has an
    # annotation.  Existing self rows are intentionally replaced by the exact
    # mathematical self term, rather than trusting rounded source values.
    if score_variants is not None:
        self_universe = score_ids.join(annotation_tags, on="variantId", how="inner")
    else:
        self_universe = (
            candidate_edges.select("variantId")
            .unionByName(annotations_long.select("variantId"))
            .distinct()
        )
    self_edges = (
        self_universe.withColumn("tagVariantId", f.col("variantId"))
        .withColumn("r", f.lit(1.0))
        .select("variantId", "tagVariantId", "r")
    )
    all_edges = candidate_edges.unionByName(self_edges)

    contributions = (
        all_edges.withColumn("r2", f.col("r") * f.col("r"))
        .join(
            annotations_long.select(
                f.col("variantId").alias("tagVariantId"),
                f.col("annotation"),
                f.col("annotationValue"),
            ),
            on="tagVariantId",
            how="inner",
        )
        .withColumn("contribution", f.col("r2") * f.col("annotationValue"))
    )
    ld_scores_long = contributions.groupBy("variantId", "annotation").agg(
        f.sum("contribution").alias("ldScore")
    )
    m_annot = annotations_long.groupBy("annotation").agg(
        f.sum("annotationValue").alias("M")
    )
    return ld_scores_long, m_annot
