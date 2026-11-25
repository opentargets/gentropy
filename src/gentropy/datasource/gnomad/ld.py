"""Read the gnomAD v2.1.1 LD matrices, for fine-mapping."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import numpy as np
from hail.linalg import BlockMatrix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row

# gnomAD v2.1.1 LD matrix (hail block matrix, GRCh37), per population.
GNOMAD_LD_MATRIX_TEMPLATE = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm"

# gnomAD v2.1.1 variant indices of the LD matrix (hail table, GRCh37), per population.
GNOMAD_LD_INDEX_RAW_TEMPLATE = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht"

# gnomAD v2.1.1 sites lifted over to GRCh38 (hail table).
GNOMAD_LIFTOVER_HT_PATH = "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht"


class GnomADLDMatrix:
    """Toolset to read the gnomAD LD matrices (version: r2.1.1) of a locus."""

    def __init__(
        self,
        ld_matrix_template: str = GNOMAD_LD_MATRIX_TEMPLATE,
        ld_index_raw_template: str = GNOMAD_LD_INDEX_RAW_TEMPLATE,
        liftover_ht_path: str = GNOMAD_LIFTOVER_HT_PATH,
    ):
        """Initialize.

        Datasets are accessed in Hail's native format, as provided by the [GnomAD consortium](https://gnomad.broadinstitute.org/downloads/#v2-linkage-disequilibrium).

        Args:
            ld_matrix_template (str): Template for the LD matrix path.
            ld_index_raw_template (str): Template for the LD index path.
            liftover_ht_path (str): Path to the liftover ht file.
        """
        self.ld_matrix_template = ld_matrix_template
        self.ld_index_raw_template = ld_index_raw_template
        self.liftover_ht_path = liftover_ht_path

    def get_numpy_matrix(
        self: GnomADLDMatrix,
        locus_index: DataFrame,
        gnomad_ancestry: str = "nfe",
    ) -> np.ndarray:
        """Extract the LD block matrix for a locus.

        Args:
            locus_index (DataFrame): hail matrix variant index table
            gnomad_ancestry (str): GnomAD major ancestry label eg. `nfe`

        Returns:
            np.ndarray: LD block matrix for the locus
        """
        idx = [row["idx"] for row in locus_index.select("idx").collect()]

        half_matrix = (
            BlockMatrix.read(self.ld_matrix_template.format(POP=gnomad_ancestry))
            .filter(idx, idx)
            .to_numpy()
        )

        return (half_matrix + half_matrix.T) - np.diag(np.diag(half_matrix))

    def get_locus_index_boundaries(
        self: GnomADLDMatrix,
        study_locus_row: Row,
        major_population: str = "nfe",
    ) -> DataFrame:
        """Extract hail matrix index from StudyLocus rows.

        Args:
            study_locus_row (Row): Study-locus row
            major_population (str): Major population to extract from gnomad matrix, default is "nfe"

        Returns:
            DataFrame: Returns the index of the gnomad matrix for the locus

        """
        chromosome = str("chr" + study_locus_row["chromosome"])
        start = int(study_locus_row["locusStart"])
        end = int(study_locus_row["locusEnd"])

        liftover_ht = hl.read_table(self.liftover_ht_path)
        liftover_ht = self._filter_liftover_by_locus(
            liftover_ht, chromosome, start, end
        )

        hail_index = hl.read_table(
            self.ld_index_raw_template.format(POP=major_population)
        )

        joined_index = (
            liftover_ht.join(hail_index, how="inner").order_by("idx").to_spark()
        )

        return joined_index

    def _filter_liftover_by_locus(
        self: GnomADLDMatrix,
        liftover_ht: hl.Table,
        chromosome: str,
        start: int,
        end: int,
    ) -> hl.Table:
        """Filter liftover hail table by locus.

        Args:
            liftover_ht (hl.Table): Liftover hail table
            chromosome (str): Chromosome of locus
            start (int): Start position for locus
            end (int): End position for locus

        Returns:
            hl.Table: Filtered liftover hail table
        """
        liftover_ht = (
            liftover_ht.filter(
                (liftover_ht.locus.contig == chromosome)
                & (liftover_ht.locus.position >= start)
                & (liftover_ht.locus.position <= end)
            )
            .key_by()
            .select("locus", "alleles", "original_locus", "original_alleles")
            .key_by("original_locus", "original_alleles")
            .naive_coalesce(20)
        )

        return liftover_ht
