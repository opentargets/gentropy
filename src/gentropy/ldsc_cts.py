"""Step to compute annotation-stratified LD scores from a specificity matrix.

This is the heavy, reusable half of the gentropy re-implementation of the
CELLECT / LDSC ``--h2-cts`` workflow. Given a single expression-specificity
matrix (genes x cell types) it produces per-variant, per-cell-type LD scores and
the per-annotation ``M`` totals. Because these artefacts only depend on the
specificity matrix and the LD reference (not on any GWAS), they are computed a
handful of times and then reused to prioritise thousands of GWAS with the
lightweight :class:`~gentropy.ldsc_cts_prioritise.CellTypeHeritabilityStep`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gentropy.common.session import Session
from gentropy.method.ldsc.cell_type_annotation import (
    build_snp_annotations,
    compute_annotation_ld_scores,
    explode_ld_index,
    map_genes_to_variants,
    melt_specificity_matrix,
)


class SpecificityLDScoreStep:
    """Compute annotation-stratified LD scores for one specificity matrix."""

    def __init__(
        self,
        session: Session,
        specificity_input_path: str,
        target_index_path: str,
        ld_index_path: str,
        output_path: str,
        specificity_id: str = "specificity",
        population: str = "nfe",
        window_kb: int = 100,
        gene_id_column: str = "gene",
        specificity_format: str = "csv",
        specificity_sep: str = ",",
        strip_gene_version: bool = True,
    ) -> None:
        """Initialise the specificity LD-score computation step.

        Args:
            session (Session): Gentropy session object.
            specificity_input_path (str): Path to the wide expression-specificity
                matrix (one gene id column plus one column per cell-type annotation).
            target_index_path (str): Path to the gentropy target index parquet used
                for gene coordinates.
            ld_index_path (str): Path to the gentropy LD index parquet.
            output_path (str): Base output directory. LD scores are written to
                ``{output_path}/ld_scores`` and per-annotation ``M`` totals to
                ``{output_path}/m_annot``.
            specificity_id (str): Identifier for this specificity matrix, stored on
                every output row. Defaults to ``"specificity"``.
            population (str): LD reference population selected from the LD index
                ``rValues``. Defaults to ``"nfe"``.
            window_kb (int): Half-window (kb) added to each side of a gene body when
                mapping genes to variants. Defaults to ``100``.
            gene_id_column (str): Name of the gene id column in the specificity
                matrix. Defaults to ``"gene"``.
            specificity_format (str): Format of the specificity matrix, either
                ``"csv"`` or ``"parquet"``. Defaults to ``"csv"``.
            specificity_sep (str): Field separator when reading a CSV specificity
                matrix. Defaults to ``","``.
            strip_gene_version (bool): Whether to strip a trailing Ensembl version
                suffix from gene ids. Defaults to ``True``.
        """
        self.session = session
        self.specificity_input_path = specificity_input_path
        self.target_index_path = target_index_path
        self.ld_index_path = ld_index_path
        self.output_path = output_path.rstrip("/")
        self.specificity_id = specificity_id
        self.population = population
        self.window_kb = window_kb
        self.gene_id_column = gene_id_column
        self.specificity_format = specificity_format
        self.specificity_sep = specificity_sep
        self.strip_gene_version = strip_gene_version

        self._run()

    def _run(self) -> None:
        """Execute the specificity LD-score computation pipeline."""
        es_matrix = self._read_specificity_matrix()

        specificity_long = melt_specificity_matrix(
            es_matrix,
            gene_id_column=self.gene_id_column,
            strip_gene_version=self.strip_gene_version,
        )
        gene_locations = self._read_gene_locations(es_matrix)

        ld_edges = explode_ld_index(
            self._read_ld_index(),
            population=self.population,
        )
        variant_positions = self._variant_positions_from_edges(ld_edges)

        gene_variant_map = map_genes_to_variants(
            gene_locations=gene_locations,
            variant_positions=variant_positions,
            window_kb=self.window_kb,
        )
        annotations_long = build_snp_annotations(
            gene_variant_map=gene_variant_map,
            specificity_long=specificity_long,
        )

        ld_scores_long, m_annot = compute_annotation_ld_scores(
            annotations_long=annotations_long,
            ld_edges=ld_edges,
        )

        self._write_outputs(ld_scores_long=ld_scores_long, m_annot=m_annot)

    def _read_specificity_matrix(self) -> DataFrame:
        """Read the wide expression-specificity matrix.

        Returns:
            DataFrame: The specificity matrix as read from disk.

        Raises:
            ValueError: If ``specificity_format`` is not ``"csv"`` or ``"parquet"``.
        """
        fmt = self.specificity_format.lower()
        if fmt == "parquet":
            return self.session.spark.read.parquet(self.specificity_input_path)
        if fmt == "csv":
            return self.session.spark.read.csv(
                self.specificity_input_path,
                header=True,
                sep=self.specificity_sep,
                inferSchema=True,
            )
        raise ValueError(
            f"Unsupported specificity_format '{self.specificity_format}'; "
            "expected 'csv' or 'parquet'."
        )

    def _read_gene_locations(self, es_matrix: DataFrame) -> DataFrame:
        """Read gene coordinates for genes present in the specificity matrix.

        Args:
            es_matrix (DataFrame): The wide specificity matrix, used to restrict
                the target index to genes present in the matrix.

        Returns:
            DataFrame: Gene coordinates with columns ``geneId``, ``chromosome``,
            ``start`` and ``end``.

        Raises:
            ValueError: If gene coordinates cannot be located in the target index.
        """
        target_df = self.session.spark.read.parquet(self.target_index_path)
        columns = set(target_df.columns)

        gene_id_col = F.col("id") if "id" in columns else F.col("geneId")
        if "genomicLocation" in columns:
            chromosome = F.col("genomicLocation.chromosome")
            start = F.col("genomicLocation.start")
            end = F.col("genomicLocation.end")
        elif {"chromosome", "start", "end"}.issubset(columns):
            chromosome = F.col("chromosome")
            start = F.col("start")
            end = F.col("end")
        else:
            raise ValueError(
                "Target index must contain 'genomicLocation' or top-level "
                "'chromosome'/'start'/'end' columns for gene coordinates."
            )

        gene_id = gene_id_col
        if self.strip_gene_version:
            gene_id = F.regexp_replace(gene_id_col, r"\.\d+$", "")

        gene_locations = (
            target_df.select(
                gene_id.alias("geneId"),
                chromosome.cast("string").alias("chromosome"),
                start.cast("long").alias("start"),
                end.cast("long").alias("end"),
            )
            .filter(F.col("chromosome").isNotNull())
            .filter(F.col("start").isNotNull())
            .filter(F.col("end").isNotNull())
            .dropDuplicates(["geneId"])
        )

        matrix_genes = self._matrix_gene_ids(es_matrix)
        return gene_locations.join(matrix_genes, on="geneId", how="inner")

    def _matrix_gene_ids(self, es_matrix: DataFrame) -> DataFrame:
        """Return the distinct gene ids present in the specificity matrix.

        The full gene set (including genes with zero specificity in every cell
        type) is required so that the control annotation covers all background
        genes.

        Args:
            es_matrix (DataFrame): The wide specificity matrix.

        Returns:
            DataFrame: Single-column dataframe of distinct ``geneId`` values.
        """
        gene_id = F.col(self.gene_id_column)
        if self.strip_gene_version:
            gene_id = F.regexp_replace(gene_id, r"\.\d+$", "")
        return es_matrix.select(gene_id.alias("geneId")).distinct()

    def _read_ld_index(self) -> DataFrame:
        """Read the LD index parquet.

        Returns:
            DataFrame: LD index with ``variantId`` and ``ldSet`` columns.
        """
        return self.session.spark.read.parquet(self.ld_index_path).select(
            "variantId", "ldSet"
        )

    @staticmethod
    def _variant_positions_from_edges(ld_edges: DataFrame) -> DataFrame:
        """Derive variant coordinates from an LD edge list by parsing variant ids.

        The gentropy ``variantId`` is formatted as
        ``chromosome_position_reference_alternate``, so coordinates can be parsed
        directly without loading the variant index.

        Args:
            ld_edges (DataFrame): Edge list with ``variantId`` and ``tagVariantId``.

        Returns:
            DataFrame: Distinct ``variantId``, ``chromosome`` and ``position`` for
            every variant that appears as either an index or a tag variant.
        """
        universe = (
            ld_edges.select("variantId")
            .union(ld_edges.select(F.col("tagVariantId").alias("variantId")))
            .distinct()
        )
        parts = F.split(F.col("variantId"), "_")
        return universe.select(
            F.col("variantId"),
            parts.getItem(0).alias("chromosome"),
            parts.getItem(1).cast("long").alias("position"),
        ).filter(F.col("position").isNotNull())

    def _write_outputs(self, ld_scores_long: DataFrame, m_annot: DataFrame) -> None:
        """Write the LD scores and per-annotation ``M`` totals to parquet.

        Args:
            ld_scores_long (DataFrame): Long LD-score table with columns
                ``variantId``, ``annotation`` and ``ldScore``.
            m_annot (DataFrame): Per-annotation ``M`` totals with columns
                ``annotation`` and ``M``.
        """
        specificity_id = F.lit(self.specificity_id)
        (
            ld_scores_long.withColumn("specificityId", specificity_id)
            .write.mode("overwrite")
            .parquet(f"{self.output_path}/ld_scores")
        )
        (
            m_annot.withColumn("specificityId", specificity_id)
            .write.mode("overwrite")
            .parquet(f"{self.output_path}/m_annot")
        )
