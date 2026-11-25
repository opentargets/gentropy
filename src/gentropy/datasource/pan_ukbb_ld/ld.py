"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import numpy as np
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix
from pyspark.sql import Window

from gentropy.common.session import Session
from gentropy.dataset.ld_index import LDIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row

# Pan-UKBB variant LD index (hail table), per population.
PAN_UKBB_HT_PATH = "gs://panukbb-ld-matrixes/ukb-diverse-pops-public-build-38/UKBB.{POP}.ldadj.variant.b38"

# Pan-UKBB LD matrix (hail block matrix), per population.
PAN_UKBB_BM_PATH = "gs://panukbb-ld-matrixes/UKBB.{POP}.ldadj"

# Pan-UKBB variant LD index with alleles aligned to the variant annotation, per population.
PAN_UKBB_ANNOTATION_PATH = "gs://panukbb-ld-matrixes/UKBB.{POP}.aligned.parquet"

# Pan-UKBB populations with an LD matrix.
PAN_UKBB_POPULATIONS = [
    "AFR",  # African
    "CSA",  # Central/South Asian
    "EUR",  # European
]

# The LD population label of each Pan-UKBB population, as in `ldPopulationStructure`.
PAN_UKBB_LD_POPULATIONS: dict[str, str] = {
    "AFR": "afr",  # African
    "AMR": "amr",  # Admixed American
    "CSA": "csa",  # Central/South Asian
    "EAS": "eas",  # East Asian
    "EUR": "nfe",  # European
    "MID": "mid",  # Middle Eastern
}

# Chromosomes kept from the Pan-UKBB variant tables, which also hold a few alternate contigs.
_CHROMOSOMES = [str(c) for c in range(1, 23)] + ["X"]


class PanUKBBLDMatrix:
    """Toolset to work with Pan UKBB LD matrices."""

    def __init__(
        self,
        pan_ukbb_ht_path: str = PAN_UKBB_HT_PATH,
        pan_ukbb_bm_path: str = PAN_UKBB_BM_PATH,
        ld_populations: list[str] = PAN_UKBB_POPULATIONS,
        ukbb_annotation_path: str = PAN_UKBB_ANNOTATION_PATH,
    ):
        """Initialize.

        Datasets are in hail native format.

        Args:
            pan_ukbb_ht_path (str): Path to hail table, source: gs://ukb-diverse-pops-public/ld_release
            pan_ukbb_bm_path (str): Path to hail block matrix
            ld_populations (list[str]): List of populations
            ukbb_annotation_path (str): Path to pan-ukbb variant LD index with alleles flipped to match the order in OT variant annotation
        """
        self.pan_ukbb_ht_path = pan_ukbb_ht_path
        self.pan_ukbb_bm_path = pan_ukbb_bm_path
        self.ld_populations = ld_populations
        self.ukbb_annotation_output_path = ukbb_annotation_path

    @staticmethod
    def read_variant_table(path: str) -> DataFrame:
        """Read a Pan-UKBB variant table (hail table, GRCh38) of an LD matrix.

        Args:
            path (str): Path to the hail table, such as ``UKBB.EUR.ldadj.variant.b38.ht``.

        Returns:
            DataFrame: The table with flattened ``locus.contig``, ``locus.position``, ``alleles`` and ``idx``.
        """
        return (
            hl.read_table(path)
            .to_spark()
            .select("`locus.contig`", "`locus.position`", "alleles", "idx")
        )

    @staticmethod
    def align_variant_table(
        variant_table: DataFrame, variant_index: DataFrame
    ) -> DataFrame:
        """Match the variants of a Pan-UKBB LD matrix to the variant index.

        The Pan-UKBB matrices were computed on GRCh37 and their variants lifted over, so
        the alleles of a variant can be swapped relative to the variant index. A variant
        is kept when the variant index holds it with the same alleles (``alleleOrder``
        1) or with them swapped (``alleleOrder`` -1, which flips the sign of its ``r``);
        when both exist, the same alleles win. Only chromosomes 1-22 and X are kept, and
        a variant ID matched by more than one matrix index is dropped as ambiguous.

        Args:
            variant_table (DataFrame): Variant table of the matrix, as read by ``read_variant_table``.
            variant_index (DataFrame): The variant index, with ``variantId``, ``chromosome``,
                ``position``, ``referenceAllele`` and ``alternateAllele``.

        Returns:
            DataFrame: ``variantId``, ``chromosome``, ``position``, ``referenceAllele``,
                ``alternateAllele``, ``alleleOrder`` and ``idx``, one row per matrix index.

        Examples:
            >>> table = spark.createDataFrame(
            ...     [("chr1", 10, ["A", "G"], 0), ("chr1", 20, ["T", "C"], 1),
            ...      ("chr1", 30, ["G", "A"], 2), ("chr1_KI270706v1_random", 5, ["A", "C"], 3)],
            ...     "`locus.contig` string, `locus.position` int, alleles array<string>, idx long",
            ... )
            >>> variants = spark.createDataFrame(
            ...     [("1_10_A_G", "1", 10, "A", "G"), ("1_20_C_T", "1", 20, "C", "T")],
            ...     "variantId string, chromosome string, position int, "
            ...     "referenceAllele string, alternateAllele string",
            ... )
            >>> aligned = PanUKBBLDMatrix.align_variant_table(table, variants)
            >>> [(r.variantId, r.alleleOrder, r.idx) for r in aligned.orderBy("idx").collect()]
            [('1_10_A_G', 1, 0), ('1_20_C_T', -1, 1)]
        """
        matrix_variants = variant_table.select(
            f.regexp_replace("`locus.contig`", "^chr", "").alias("chromosome"),
            f.col("`locus.position`").alias("position"),
            f.col("alleles").getItem(0).alias("matrixRef"),
            f.col("alleles").getItem(1).alias("matrixAlt"),
            "idx",
        ).where(f.col("chromosome").isin(_CHROMOSOMES))
        same = (f.col("matrixRef") == f.col("referenceAllele")) & (
            f.col("matrixAlt") == f.col("alternateAllele")
        )
        swapped = (f.col("matrixRef") == f.col("alternateAllele")) & (
            f.col("matrixAlt") == f.col("referenceAllele")
        )
        matched = (
            matrix_variants.join(
                variant_index.select(
                    "variantId",
                    "chromosome",
                    "position",
                    "referenceAllele",
                    "alternateAllele",
                ),
                on=["chromosome", "position"],
            )
            .where(same | swapped)
            .withColumn("alleleOrder", f.when(same, 1).otherwise(-1))
            # One variant per matrix index, preferring the same alleles:
            .withColumn(
                "rank",
                f.row_number().over(
                    Window.partitionBy("idx").orderBy(f.col("alleleOrder").desc())
                ),
            )
            .where(f.col("rank") == 1)
            # One matrix index per variant:
            .withColumn("indices", f.count("idx").over(Window.partitionBy("variantId")))
            .where(f.col("indices") == 1)
        )
        return matched.select(
            "variantId",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
            "alleleOrder",
            "idx",
        )

    @staticmethod
    def _resolve_pairs(
        pairs: DataFrame, aligned_variants: DataFrame, ld_population: str
    ) -> DataFrame:
        """Turn matrix entries into LD index rows: variant IDs, signed r, both orders.

        The matrix stores one triangle; whichever it is, each unordered pair is kept
        once, then written in both orders. A pair is kept only when both variants are
        aligned and on the same chromosome, and its ``r`` is multiplied by the allele
        orders of both variants.

        Args:
            pairs (DataFrame): Matrix entries ``i``, ``j`` and ``r``.
            aligned_variants (DataFrame): The matrix variants, as from ``align_variant_table``.
            ld_population (str): LD population label of the rows.

        Returns:
            DataFrame: Rows of the LD index schema.

        Examples:
            >>> pairs = spark.createDataFrame(
            ...     [(0, 0, 1.0), (1, 0, 0.8), (0, 1, 0.8), (1, 1, 1.0), (2, 0, 0.9)],
            ...     "i long, j long, r double",
            ... )
            >>> aligned = spark.createDataFrame(
            ...     [("1_10_A_G", "1", 1, 0), ("1_20_C_T", "1", -1, 1), ("2_5_A_C", "2", 1, 2)],
            ...     "variantId string, chromosome string, alleleOrder int, idx long",
            ... )
            >>> rows = PanUKBBLDMatrix._resolve_pairs(pairs, aligned, "nfe")
            >>> sorted((r.variantId, r.tagVariantId, r.r) for r in rows.collect())
            [('1_10_A_G', '1_10_A_G', 1.0), ('1_10_A_G', '1_20_C_T', -0.8), ('1_20_C_T', '1_10_A_G', -0.8), ('1_20_C_T', '1_20_C_T', 1.0)]
        """
        unordered = pairs.select(
            f.least("i", "j").alias("i"), f.greatest("i", "j").alias("j"), "r"
        ).dropDuplicates(["i", "j"])
        first = aligned_variants.select(
            f.col("idx").alias("i"),
            "variantId",
            "chromosome",
            f.col("alleleOrder").alias("orderI"),
        )
        second = aligned_variants.select(
            f.col("idx").alias("j"),
            f.col("variantId").alias("tagVariantId"),
            f.col("chromosome").alias("tagChromosome"),
            f.col("alleleOrder").alias("orderJ"),
        )
        one_way = (
            unordered.join(first, on="i")
            .join(second, on="j")
            .where(f.col("chromosome") == f.col("tagChromosome"))
            .select(
                f.lit(ld_population).alias("population"),
                "chromosome",
                "variantId",
                "tagVariantId",
                (f.col("r") * f.col("orderI") * f.col("orderJ"))
                .cast("double")
                .alias("r"),
            )
        )
        other_way = one_way.where(f.col("variantId") != f.col("tagVariantId")).select(
            "population",
            "chromosome",
            f.col("tagVariantId").alias("variantId"),
            f.col("variantId").alias("tagVariantId"),
            "r",
        )
        return one_way.unionByName(other_way)

    def as_ld_index(
        self: PanUKBBLDMatrix,
        population: str,
        aligned_variants: DataFrame,
        min_r2: float,
        band_blocks: int,
    ) -> LDIndex:
        """Build the LD index of one Pan-UKBB population.

        Only the blocks within ``band_blocks`` block-diagonals of the main diagonal are
        read: pairs at high ``r`` are close, and the further blocks make up most of the
        matrix. Entries with ``|r|`` of at least ``sqrt(min_r2)`` are kept, including
        each variant with itself.

        Args:
            population (str): Pan-UKBB population, such as ``EUR``.
            aligned_variants (DataFrame): The population's matrix variants, as from ``align_variant_table``.
            min_r2 (float): Smallest r2 kept.
            band_blocks (int): Block-diagonals read on each side of the main diagonal.

        Returns:
            LDIndex: The population's pairs.
        """
        block_matrix = BlockMatrix.read(self.pan_ukbb_bm_path.format(POP=population))
        width = band_blocks * block_matrix.block_size
        entries = block_matrix.sparsify_band(
            lower=-width, upper=width, blocks_only=True
        ).entries(keyed=False)
        pairs = (
            entries.filter(hl.abs(entries.entry) >= min_r2**0.5)
            .to_spark()
            .withColumnRenamed("entry", "r")
        )
        return LDIndex(
            _df=self._resolve_pairs(
                pairs, aligned_variants, PAN_UKBB_LD_POPULATIONS[population]
            ),
            _schema=LDIndex.get_schema(),
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

    def _load_hail_block_matrix(
        self: PanUKBBLDMatrix,
        idx: list[int],
        ancestry: str,
    ) -> np.ndarray:
        """Read the rows and columns of the population's LD matrix at the given indices.

        Args:
            idx (list[int]): Indices of the variants in the LD matrix.
            ancestry (str): Pan-UKBB population.

        Returns:
            np.ndarray: The upper triangle of the LD matrix of the variants.
        """
        return (
            BlockMatrix.read(self.pan_ukbb_bm_path.format(POP=ancestry))
            .filter(idx, idx)
            .to_numpy()
        )

    def _get_outer_allele_order(
        self: PanUKBBLDMatrix, locus_index: DataFrame
    ) -> np.ndarray:
        """Outer product of the allele orders, which flips the sign of r for swapped alleles.

        Args:
            locus_index (DataFrame): Variant LD index of the locus, with `alleleOrder`.

        Returns:
            np.ndarray: Sign matrix, with ones on the diagonal.
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
        """Build the symmetric LD matrix from its upper triangle, with alleles aligned.

        Args:
            half_matrix (np.ndarray): Upper triangle of the LD matrix.
            outer_allele_order (np.ndarray): Sign matrix of the allele orders.

        Returns:
            np.ndarray: Symmetric LD matrix, with ones on the diagonal.
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
