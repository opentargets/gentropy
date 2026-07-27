"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import numpy as np
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix
from pyspark.sql.window import Window

from gentropy.common.session import Session
from gentropy.config import PanUKBBConfig
from gentropy.dataset.multi_ancestry_pairwise_ld import MultiAncestryPairwiseLD

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row


DEFAULT_PAN_UKBB_BM_PATH = "gs://panukbb-ld-matrixes/UKBB.{POP}.ldadj"


def normalize_pan_ukbb_population(population: str) -> str:
    """Normalize pipeline ancestry labels to PanUKBB population labels.

    Args:
        population (str): Pipeline ancestry alias or PanUKBB population label.

    Returns:
        str: Normalized PanUKBB population label.
    """
    lowered_population = population.lower()
    if lowered_population == "nfe":
        return "EUR"

    upper_population = population.upper()
    if upper_population in {"AFR", "CSA", "EUR"}:
        return upper_population

    raise ValueError(f"Unsupported PanUKBB population: {population}")


class PanUKBBLDMatrix:
    """Toolset to work with Pan UKBB LD matrices."""

    def __init__(
        self,
        pan_ukbb_ht_path: str = PanUKBBConfig().pan_ukbb_ht_path,
        pan_ukbb_bm_path: str = DEFAULT_PAN_UKBB_BM_PATH,
        ld_populations: list[str] = PanUKBBConfig().pan_ukbb_pops,
        ukbb_annotation_path: str = PanUKBBConfig().ukbb_annotation_path,
    ):
        """Initialize.

        Datasets are in hail native format.

        Args:
            pan_ukbb_ht_path (str): Path to hail table, source: gs://ukb-diverse-pops-public/ld_release
            pan_ukbb_bm_path (str): Path to hail block matrix
            ld_populations (list[str]): List of populations
            ukbb_annotation_path (str): Path to pan-ukbb variant LD index with alleles flipped to match the order in OT variant annotation
        Default variant-table and annotation paths are set in PanUKBBConfig.
        """
        self.pan_ukbb_ht_path = pan_ukbb_ht_path
        self.pan_ukbb_bm_path = pan_ukbb_bm_path
        self.ld_populations = [
            normalize_pan_ukbb_population(population) for population in ld_populations
        ]
        self.ukbb_annotation_output_path = ukbb_annotation_path

    def align_ld_index_alleles(
        self,
        variant_annotation: DataFrame,
        population: str,
        hail_table_path: str = PanUKBBConfig.pan_ukbb_ht_path,
        hail_table_output: str = PanUKBBConfig.ukbb_annotation_path,
    ) -> None:
        """Align Pan-UKBB variant LD index alleles with the Open Targets variant annotation.

        Args:
            variant_annotation (DataFrame): Open Targets variant annotation DataFrame
            population (str): Population label
            hail_table_path (str): Path to hail table with Pan-UKBB variant LD index
            hail_table_output (str): Path to output the aligned Pan-UKBB variant LD index with alleles in the correct order
        """
        population = normalize_pan_ukbb_population(population)
        raw_index = (
            hl.read_table(hail_table_path.format(POP=population))
            .to_spark()
            .select(
                "`locus.contig`",
                "`locus.position`",
                "`alleles`",
                "`idx`",
            )
        )
        self.prepare_aligned_ld_index(
            raw_index=raw_index,
            variant_annotation=variant_annotation,
            population=population,
        ).write.mode("overwrite").parquet(hail_table_output.format(POP=population))

    @staticmethod
    def prepare_aligned_ld_index(
        raw_index: DataFrame,
        variant_annotation: DataFrame,
        population: str,
    ) -> DataFrame:
        """Prepare a PanUKBB LD variant index aligned to Open Targets alleles.

        Args:
            raw_index (DataFrame): PanUKBB LD variant table converted from Hail.
            variant_annotation (DataFrame): Open Targets variant annotation.
            population (str): PanUKBB population label.

        Returns:
            DataFrame: Prepared LD index with variant ID, matrix index, population, and allele orientation.
        """
        ht = (
            raw_index.withColumns(
                {
                    "chromosome": f.split("`locus.contig`", "chr")[1],
                    "position": f.col("`locus.position`"),
                    "referenceAllele": f.element_at("`alleles`", 1),
                    "alternateAllele": f.element_at("`alleles`", 2),
                }
            )
            .drop("locus.contig", "locus.position", "alleles")
            .dropDuplicates(
                ["chromosome", "position", "referenceAllele", "alternateAllele"]
            )
        )
        va = variant_annotation.select(
            "chromosome",
            "position",
            f.col("referenceAllele").alias("va_ref"),
            f.col("alternateAllele").alias("va_alt"),
        ).dropDuplicates(["chromosome", "position", "va_ref", "va_alt"])
        va_positions = (
            va.select("chromosome", "position")
            .dropDuplicates()
            .withColumn("has_variant_annotation_at_position", f.lit(True))
        )
        va_orientations = (
            va.select(
                "chromosome",
                "position",
                f.col("va_ref").alias("referenceAllele"),
                f.col("va_alt").alias("alternateAllele"),
                f.col("va_ref").alias("new_referenceAllele"),
                f.col("va_alt").alias("new_alternateAllele"),
                f.lit(1).alias("matchedAlleleOrder"),
            )
            .unionByName(
                va.select(
                    "chromosome",
                    "position",
                    f.col("va_alt").alias("referenceAllele"),
                    f.col("va_ref").alias("alternateAllele"),
                    f.col("va_ref").alias("new_referenceAllele"),
                    f.col("va_alt").alias("new_alternateAllele"),
                    f.lit(-1).alias("matchedAlleleOrder"),
                )
            )
            .dropDuplicates(
                ["chromosome", "position", "referenceAllele", "alternateAllele"]
            )
        )
        ht_va = (
            ht.join(
                va_orientations,
                on=["chromosome", "position", "referenceAllele", "alternateAllele"],
                how="left",
            )
            .join(va_positions, on=["chromosome", "position"], how="left")
            .filter(
                f.col("matchedAlleleOrder").isNotNull()
                | f.col("has_variant_annotation_at_position").isNull()
            )
            .select(
                f.concat_ws(
                    "_",
                    "chromosome",
                    "position",
                    f.coalesce("new_referenceAllele", "referenceAllele"),
                    f.coalesce("new_alternateAllele", "alternateAllele"),
                ).alias("variantId"),
                "chromosome",
                "position",
                f.coalesce("new_referenceAllele", "referenceAllele").alias(
                    "referenceAllele"
                ),
                f.coalesce("new_alternateAllele", "alternateAllele").alias(
                    "alternateAllele"
                ),
                "idx",
                f.lit(population).alias("population"),
                f.coalesce("matchedAlleleOrder", f.lit(1)).alias("alleleOrder"),
            )
        )
        window_spec = Window.partitionBy("idx").orderBy(f.col("alleleOrder").desc())
        return (
            ht_va.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .drop("rank")
        )

    @staticmethod
    def filter_ld_index_to_variants(
        ld_index: DataFrame,
        variants: DataFrame,
    ) -> DataFrame:
        """Filter a prepared PanUKBB LD index to a requested variant set.

        Args:
            ld_index (DataFrame): Prepared PanUKBB LD index.
            variants (DataFrame): Variant set containing variantId and chromosome.

        Returns:
            DataFrame: Filtered LD index preserving the input schema.
        """
        requested_variants = variants.select("variantId", "chromosome").dropDuplicates()
        return (
            ld_index.join(
                requested_variants,
                on=["variantId", "chromosome"],
                how="inner",
            )
            .select(*ld_index.columns)
            .sort("idx")
        )

    def get_numpy_matrix(
        self: PanUKBBLDMatrix,
        locus_index: DataFrame,
        ancestry: str,
    ) -> np.ndarray:
        """Extract the LD block matrix for a locus.

        Args:
            locus_index (DataFrame): hail matrix variant index table
            ancestry (str): Ancestry label

        Returns:
            np.ndarray: LD block matrix for the locus
        """
        idx = [row["idx"] for row in locus_index.select("idx").collect()]
        half_matrix = self._load_hail_block_matrix(idx, ancestry)
        outer_allele_order = self._get_outer_allele_order(locus_index)
        ld_matrix = self._construct_ld_matrix(half_matrix, outer_allele_order)
        return ld_matrix

    def get_long_format_ld_matrix(
        self: PanUKBBLDMatrix,
        locus_index: DataFrame,
        ancestry: str,
    ) -> DataFrame:
        """Extract signed long-format LD pairs for the supplied prepared PanUKBB index.

        Args:
            locus_index (DataFrame): Prepared and filtered PanUKBB LD index.
            ancestry (str): Ancestry label for the requested LD matrix.

        Returns:
            DataFrame: Long-format LD pairs with ancestry, variantIdI, variantIdJ, and signed r.
        """
        output_ancestry = (
            normalize_pan_ukbb_population(ancestry)
            if "{POP}" in self.pan_ukbb_bm_path
            else ancestry
        )
        ordered_locus_index = locus_index.sort("idx", "variantId")
        return self._get_distributed_long_format_ld_matrix(
            ordered_locus_index, output_ancestry
        )

    def _get_distributed_long_format_ld_matrix(
        self,
        ordered_locus_index: DataFrame,
        ancestry: str,
    ) -> DataFrame:
        """Extract sparse signed LD pairs without materialising a dense matrix.

        The PanUKBB block matrices store one triangular half.  Hail entries are
        converted directly to Spark rows, non-zero upper-triangle entries are
        mirrored, and allele orientation is applied by distributed joins.  The
        diagonal is emitted explicitly because it is required even when a
        source matrix stores it sparsely.

        Args:
            ordered_locus_index (DataFrame): Sorted, prepared LD index for one locus.
            ancestry (str): Normalized ancestry label for the matrix.

        Returns:
            DataFrame: Sparse signed LD pairs with both matrix directions.
        """
        index_rows = ordered_locus_index.select(
            "idx", "variantId", "alleleOrder"
        ).collect()
        if not index_rows:
            return ordered_locus_index.sparkSession.createDataFrame(
                [],
                "ancestry string, variantIdI string, variantIdJ string, r double",
            )

        matrix_indices = [row["idx"] for row in index_rows]
        position_rows = [
            (position, row["variantId"], row["alleleOrder"])
            for position, row in enumerate(index_rows)
        ]
        spark = ordered_locus_index.sparkSession
        positions = spark.createDataFrame(
            position_rows,
            "position int, variantId string, alleleOrder int",
        )
        entries_table = self._filter_hail_block_matrix(
            matrix_indices, ancestry
        ).entries(keyed=False)
        entries = (
            entries_table.filter(
                (entries_table.i < entries_table.j) & (entries_table.entry != 0)
            )
            .to_spark()
            .select(
                f.col("i").cast("int"),
                f.col("j").cast("int"),
                f.col("entry").cast("double"),
            )
        )
        diagonal = positions.select(
            f.col("position").alias("i"),
            f.col("position").alias("j"),
            f.lit(1.0).alias("entry"),
        )
        canonical = diagonal.unionByName(entries)
        pairs = canonical.unionByName(
            entries.select(
                f.col("j").alias("i"),
                f.col("i").alias("j"),
                "entry",
            )
        )
        left = positions.select(
            f.col("position").alias("i"),
            f.col("variantId").alias("variantIdI"),
            f.col("alleleOrder").alias("alleleOrderI"),
        )
        right = positions.select(
            f.col("position").alias("j"),
            f.col("variantId").alias("variantIdJ"),
            f.col("alleleOrder").alias("alleleOrderJ"),
        )
        return (
            pairs.join(left, on="i")
            .join(right, on="j")
            .select(
                f.lit(ancestry).alias("ancestry"),
                "variantIdI",
                "variantIdJ",
                f.when(f.col("i") == f.col("j"), f.lit(1.0))
                .otherwise(
                    f.col("entry") * f.col("alleleOrderI") * f.col("alleleOrderJ")
                )
                .cast("double")
                .alias("r"),
            )
        )

    def write_long_format_ld_matrix(
        self: PanUKBBLDMatrix,
        locus_index: DataFrame,
        ancestry: str,
        output_path: str,
        write_mode: str = "errorifexists",
    ) -> None:
        """Write signed long-format LD pairs for the supplied prepared PanUKBB index.

        Args:
            locus_index (DataFrame): Prepared and filtered PanUKBB LD index.
            ancestry (str): Ancestry label for the requested LD matrix.
            output_path (str): Output parquet path.
            write_mode (str): Spark write mode.
        """
        self.get_long_format_ld_matrix(locus_index, ancestry).write.mode(
            write_mode
        ).parquet(output_path)

    def get_multi_ancestry_long_format_ld_matrix(
        self: PanUKBBLDMatrix,
        locus_indexes: dict[str, DataFrame],
    ) -> MultiAncestryPairwiseLD:
        """Extract and combine long-format LD pairs for multiple ancestries.

        Args:
            locus_indexes (dict[str, DataFrame]): One prepared locus index per
                ancestry. Each index must contain the matrix indexes and allele
                orientation required by :meth:`get_long_format_ld_matrix`.

        Returns:
            MultiAncestryPairwiseLD: Combined ancestry-aware pairwise LD dataset.
        """
        if not locus_indexes:
            raise ValueError("At least one ancestry-specific locus index is required")

        ancestry_order = sorted(locus_indexes)
        normalized_ancestry_order = [
            normalize_pan_ukbb_population(ancestry) for ancestry in ancestry_order
        ]
        if len(set(normalized_ancestry_order)) != len(normalized_ancestry_order):
            raise ValueError("Locus indexes must contain each ancestry at most once")

        pairwise_ld = [
            self.get_long_format_ld_matrix(locus_indexes[ancestry], ancestry)
            for ancestry in ancestry_order
        ]
        combined = pairwise_ld[0]
        for pairs in pairwise_ld[1:]:
            combined = combined.unionByName(pairs)
        return MultiAncestryPairwiseLD(
            _df=combined,
            _schema=MultiAncestryPairwiseLD.get_schema(),
        )

    def _load_hail_block_matrix(
        self: PanUKBBLDMatrix,
        idx: list[int],
        ancestry: str,
    ) -> np.ndarray:
        """Load a filtered PanUKBB Hail BlockMatrix slice as a NumPy array.

        Args:
            idx (list[int]): Matrix indices to keep.
            ancestry (str): PanUKBB ancestry label.

        Returns:
            np.ndarray: Filtered LD matrix slice.
        """
        return self._filter_hail_block_matrix(idx, ancestry).to_numpy()

    def _filter_hail_block_matrix(
        self: PanUKBBLDMatrix,
        idx: list[int],
        ancestry: str,
    ) -> BlockMatrix:
        """Read and filter a PanUKBB BlockMatrix before materialisation.

        Args:
            idx (list[int]): Matrix indices to keep.
            ancestry (str): PanUKBB ancestry label.

        Returns:
            BlockMatrix: Filtered block matrix slice.
        """
        matrix_path = self.pan_ukbb_bm_path
        if "{POP}" in matrix_path:
            matrix_path = matrix_path.format(
                POP=normalize_pan_ukbb_population(ancestry)
            )
        return BlockMatrix.read(matrix_path).filter(idx, idx)

    def write_filtered_block_matrix(
        self: PanUKBBLDMatrix,
        locus_index: DataFrame,
        ancestry: str,
        output_path: str,
    ) -> None:
        """Write a bounded PanUKBB BlockMatrix slice for the provided index.

        Args:
            locus_index (DataFrame): Prepared LD index rows to keep.
            ancestry (str): PanUKBB ancestry label.
            output_path (str): Output Hail BlockMatrix path.
        """
        idx = [
            row["idx"]
            for row in locus_index.select("idx").dropDuplicates().sort("idx").collect()
        ]
        self._filter_hail_block_matrix(idx, ancestry).write(output_path, overwrite=True)

    def _get_outer_allele_order(
        self: PanUKBBLDMatrix, locus_index: DataFrame
    ) -> np.ndarray:
        """Build the pairwise allele-orientation matrix for a locus index.

        Args:
            locus_index (DataFrame): Prepared PanUKBB LD index rows.

        Returns:
            np.ndarray: Outer product of allele-order signs with unit diagonal.
        """
        alleleOrder = [
            row["alleleOrder"] for row in locus_index.select("alleleOrder").collect()
        ]
        outer_allele_order = np.outer(alleleOrder, alleleOrder)
        np.fill_diagonal(outer_allele_order, 1)
        return outer_allele_order

    def _construct_ld_matrix(
        self: PanUKBBLDMatrix,
        half_matrix: np.ndarray,
        outer_allele_order: np.ndarray,
    ) -> np.ndarray:
        """Construct a signed symmetric LD matrix from a triangular matrix.

        Args:
            half_matrix (np.ndarray): Upper-triangular LD matrix returned by Hail.
            outer_allele_order (np.ndarray): Pairwise allele-orientation signs.

        Returns:
            np.ndarray: Signed symmetric LD matrix with unit diagonal.
        """
        ld_matrix = (half_matrix + half_matrix.T) - np.diag(np.diag(half_matrix))
        ld_matrix = ld_matrix * outer_allele_order
        np.fill_diagonal(ld_matrix, 1)
        return ld_matrix

    def get_locus_index_boundaries(
        self,
        session: Session,
        study_locus_row: Row,
        ancestry: str = "EUR",
    ) -> DataFrame:
        """Extract hail matrix index from StudyLocus rows.

        Args:
            session (Session): Session object
            study_locus_row (Row): Study-locus row
            ancestry (str): Major population, default is "EUR"

        Returns:
            DataFrame: Returns the index of the pan-ukbb matrix for the locus

        """
        ancestry = normalize_pan_ukbb_population(ancestry)
        chromosome = str(study_locus_row["chromosome"])
        start = int(study_locus_row["locusStart"])
        end = int(study_locus_row["locusEnd"])

        index_file = session.spark.read.parquet(
            self.ukbb_annotation_output_path.format(POP=ancestry)
        )

        index_file = index_file.filter(
            (f.col("chromosome") == chromosome)
            & (f.col("position") >= start)
            & (f.col("position") <= end)
        ).sort("idx")

        return index_file
