"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row


class LDMatrixInterface:
    """Toolset to interact with LD matrices."""

    ancestry_map = {
        "nfe": "EUR",
        "csa": "CSA",
        "afr": "AFR",
    }

    @staticmethod
    def get_locus_index_boundaries(
        session: Session,
        study_locus_row: Row,
        ancestry: str = "nfe",
    ) -> DataFrame:
        """Extract hail matrix index from StudyLocus rows.

        Args:
            session (Session): Session object
            study_locus_row (Row): Study-locus row
            ancestry (str): Major population to extract from gnomad matrix, default is "nfe"

        Returns:
            DataFrame: Returns the index of the gnomad matrix for the locus

        """
        if ancestry in ("nfe", "csa", "afr"):
            joined_index = PanUKBBLDMatrix().get_locus_index_boundaries(
                session=session,
                study_locus_row=study_locus_row,
                ancestry=LDMatrixInterface.ancestry_map.get(ancestry, ancestry),
            )
        else:
            joined_index = (
                GnomADLDMatrix()
                .get_locus_index_boundaries(
                    study_locus_row=study_locus_row,
                    major_population=ancestry,
                )
                .withColumn(
                    "variantId",
                    f.concat(
                        f.regexp_replace(f.col("`locus.contig`"), "chr", ""),
                        f.lit("_"),
                        f.col("`locus.position`"),
                        f.lit("_"),
                        f.col("alleles").getItem(0),
                        f.lit("_"),
                        f.col("alleles").getItem(1),
                    ).cast("string"),
                )
            )

        return joined_index

    @staticmethod
    def get_numpy_matrix(
        locus_index: DataFrame,
        ancestry: str = "nfe",
    ) -> np.ndarray:
        """Extract the LD block matrix for a locus.

        Args:
            locus_index (DataFrame): hail matrix variant index table
            ancestry (str): major ancestry label eg. `nfe`

        Returns:
            np.ndarray: LD block matrix for the locus
        """
        if ancestry in (
            "afr",
            "csa",
            "nfe",
        ):
            block_matrix = PanUKBBLDMatrix().get_numpy_matrix(
                locus_index=locus_index,
                ancestry=LDMatrixInterface.ancestry_map.get(ancestry, ancestry),
            )
        else:
            block_matrix = GnomADLDMatrix.get_numpy_matrix(
                locus_index=locus_index, gnomad_ancestry=ancestry
            )

        return block_matrix
