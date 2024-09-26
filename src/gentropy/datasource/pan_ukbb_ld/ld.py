"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import numpy as np
from hail.linalg import BlockMatrix

from gentropy.config import PanUKBBConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row


class PanUKBBLDMatrix:
    """Toolset to work with Pan UKBB LD matrices."""

    def __init__(
        self,
        pan_ukbb_ht_path: str = PanUKBBConfig().pan_ukbb_ht_path,
        pan_ukbb_bm_path: str = PanUKBBConfig().pan_ukbb_bm_path,
        ld_populations: list[str] = PanUKBBConfig().pan_ukbb_pops,
    ):
        """Initialize.

        Datasets are in hail native format.

        Args:
            pan_ukbb_ht_path (str): Path to hail table
            pan_ukbb_bm_path (str): Path to hail block matrix
            ld_populations (list[str]): List of populations
        Default values are set in PanUKBBConfig.
        """
        self.pan_ukbb_ht_path = pan_ukbb_ht_path
        self.pan_ukbb_bm_path = pan_ukbb_bm_path
        self.ld_populations = ld_populations

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

        if ancestry == "nfe":
            half_matrix = (
                BlockMatrix.read(self.pan_ukbb_bm_path.format(POP="EUR"))
                .filter(idx, idx)
                .to_numpy()
            )

            alleleOrder = [
                row["alleleOrder"]
                for row in locus_index.select("alleleOrder").collect()
            ]
            outer_allele_order = np.outer(alleleOrder, alleleOrder)
            np.fill_diagonal(outer_allele_order, 1)

            ld_matrix = (half_matrix + half_matrix.T) - np.diag(np.diag(half_matrix))
            ld_matrix = ld_matrix * outer_allele_order
            np.fill_diagonal(ld_matrix, 1)
        elif ancestry == "csa":
            half_matrix = (
                BlockMatrix.read("gs://panukbb-ld-matrixes/UKBB.CSA.ldadj")
                .filter(idx, idx)
                .to_numpy()
            )

            alleleOrder = [
                row["alleleOrder"]
                for row in locus_index.select("alleleOrder").collect()
            ]
            outer_allele_order = np.outer(alleleOrder, alleleOrder)
            np.fill_diagonal(outer_allele_order, 1)

            ld_matrix = (half_matrix + half_matrix.T) - np.diag(np.diag(half_matrix))
            ld_matrix = ld_matrix * outer_allele_order
            np.fill_diagonal(ld_matrix, 1)
        else:
            ld_matrix = None

        return ld_matrix

    def get_locus_index_boundaries(
        self,
        study_locus_row: Row,
        ancestry: str = "EUR",
    ) -> DataFrame:
        """Extract hail matrix index from StudyLocus rows.

        Args:
            study_locus_row (Row): Study-locus row
            ancestry (str): Major population to extract from gnomad matrix, default is "nfe"

        Returns:
            DataFrame: Returns the index of the gnomad matrix for the locus

        """
        chromosome = str("chr" + study_locus_row["chromosome"])
        start = int(study_locus_row["locusStart"])
        end = int(study_locus_row["locusEnd"])

        index_file = hl.read_table(self.pan_ukbb_ht_path.format(POP=ancestry))

        index_file = (
            index_file.filter(
                (index_file.locus.contig == chromosome)
                & (index_file.locus.position >= start)
                & (index_file.locus.position <= end)
            )
            .order_by("idx")
            .to_spark()
        )

        return index_file
