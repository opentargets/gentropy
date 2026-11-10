"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix

from gentropy.common.session import Session
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
        ukbb_annotation_path: str = PanUKBBConfig().ukbb_annotation_path,
    ):
        """Initialize.

        Datasets are in hail native format.

        Args:
            pan_ukbb_ht_path (str): Path to hail table, source: gs://ukb-diverse-pops-public/ld_release
            pan_ukbb_bm_path (str): Path to hail block matrix
            ld_populations (list[str]): List of populations
            ukbb_annotation_path (str): Path to pan-ukbb variant LD index with alleles flipped to match the order in OT variant annotation
        Default values are set in PanUKBBConfig.
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
        return (
            BlockMatrix.read(self.pan_ukbb_bm_path.format(POP=ancestry))
            .filter(idx, idx)
            .to_numpy()
        )

    def _get_outer_allele_order(
        self: PanUKBBLDMatrix, locus_index: DataFrame
    ) -> np.ndarray:
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
