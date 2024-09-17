"""Step to import filtered version of a LD matrix (block matrix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix

from gentropy.common.session import Session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row


class PanUKBBLDMatrix:
    """Toolset ot interact with GnomAD LD dataset (version: r2.1.1)."""

    @staticmethod
    def get_numpy_matrix(
        locus_index: DataFrame,
        ancestry: str = "nfe",
    ) -> np.ndarray:
        """Extract the LD block matrix for a locus.

        Args:
            locus_index (DataFrame): hail matrix variant index table
            ancestry (str): Ancestry label eg. `nfe`

        Returns:
            np.ndarray: LD block matrix for the locus
        """
        idx = [row["idx"] for row in locus_index.select("idx").collect()]

        if ancestry == "nfe":
            half_matrix = (
                BlockMatrix.read("gs://panukbb-ld-matrixes/UKBB.EUR.ldadj")
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
        chromosome = str("chr" + study_locus_row["chromosome"])
        start = int(study_locus_row["locusStart"])
        end = int(study_locus_row["locusEnd"])

        if ancestry == "nfe":
            index_file = session.spark.read.parquet(
                "gs://genetics-portal-dev-analysis/yt4/UKBB_PAN_LD/UKBB.EUR.ldadj.variant.final.parquet"
            )

            index_file = index_file.filter(
                (f.col("locus_GRCh38_contig") == chromosome)
                & (f.col("locus_GRCh38_position") >= start)
                & (f.col("locus_GRCh38_position") <= end)
            )
        else:
            index_file = None

        return index_file
