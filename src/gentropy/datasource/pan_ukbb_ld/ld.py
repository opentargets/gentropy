"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix

from gentropy.common.session import Session

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
