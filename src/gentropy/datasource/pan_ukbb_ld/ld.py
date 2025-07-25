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
        ht = hl.read_table(hail_table_path.format(POP=population))
        ht = (
            ht.to_spark()
            .select(
                "`locus.contig`",
                "`locus.position`",
                "`alleles`",
                "`idx`",
            )
            .withColumns(
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
        ht_va = (
            ht.alias("ukbb")
            .join(
                variant_annotation.select(
                    "chromosome",
                    "position",
                    f.col("referenceAllele").alias("va_ref"),
                    f.col("alternateAllele").alias("va_alt"),
                ).dropDuplicates(["chromosome", "position", "va_ref", "va_alt"]),
                on=["chromosome", "position"],
                how="left",
            )
            .filter(
                (
                    (f.col("referenceAllele") == f.col("va_ref"))
                    & (f.col("alternateAllele") == f.col("va_alt"))
                )
                | (
                    (f.col("referenceAllele") == f.col("va_alt"))
                    & (f.col("alternateAllele") == f.col("va_ref"))
                )
                | (f.col("va_ref").isNull() | f.col("va_alt").isNull())
            )
            .withColumns(
                {
                    "alleleOrder": f.when(
                        (f.col("referenceAllele") == f.col("va_alt"))
                        & (f.col("alternateAllele") == f.col("va_ref")),
                        -1,
                    ).otherwise(1),
                    "new_referenceAllele": f.when(
                        (f.col("referenceAllele") == f.col("va_alt"))
                        & (f.col("alternateAllele") == f.col("va_ref")),
                        f.col("va_ref"),
                    ).otherwise(f.col("referenceAllele")),
                    "new_alternateAllele": f.when(
                        (f.col("alternateAllele") == f.col("va_ref"))
                        & (f.col("referenceAllele") == f.col("va_alt")),
                        f.col("va_alt"),
                    ).otherwise(f.col("alternateAllele")),
                }
            )
            .select(
                f.concat_ws(
                    "_",
                    "chromosome",
                    "position",
                    "new_referenceAllele",
                    "new_alternateAllele",
                ).alias("variantId"),
                "chromosome",
                "position",
                f.col("new_referenceAllele").alias("referenceAllele"),
                f.col("new_alternateAllele").alias("alternateAllele"),
                "alleleOrder",
                "idx",
            )
        )
        window_spec = Window.partitionBy("idx").orderBy(f.col("alleleOrder").desc())
        ht_va = (
            ht_va.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .drop("rank")
        )
        ht_va.write.mode("overwrite").parquet(hail_table_output.format(POP=population))

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
