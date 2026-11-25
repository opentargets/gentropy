"""Steps to build the LD index from the Pan-UKBB and FinnGen LD matrices."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.config import LDIndexConfig, PanUKBBLDAlignmentConfig
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.finngen.ld import FinnGenLDMatrix
from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class PanUKBBLDAlignmentStep:
    """Match the variants of each Pan-UKBB LD matrix to the variant index.

    The aligned variant tables map a matrix index to a variant ID, with the allele
    order that signs its ``r``. Both the LD index and the fine-mapping read them.
    """

    def __init__(
        self,
        session: Session,
        variant_index_path: str,
        aligned_variant_table_template: str,
        variant_table_template: str = PanUKBBLDAlignmentConfig().variant_table_template,
        populations: list[str] = PanUKBBLDAlignmentConfig().populations,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            variant_index_path (str): Path to the variant index.
            aligned_variant_table_template (str): Output path of each population's aligned
                variant table, with a ``{POP}`` placeholder.
            variant_table_template (str): Path of each population's Pan-UKBB variant table
                (hail table, GRCh38), with a ``{POP}`` placeholder.
            populations (list[str]): Pan-UKBB populations to align, such as ``EUR``.
        """
        variant_index = VariantIndex.from_parquet(session, variant_index_path)
        for population in populations:
            (
                PanUKBBLDMatrix.align_variant_table(
                    PanUKBBLDMatrix.read_variant_table(
                        variant_table_template.format(POP=population)
                    ),
                    variant_index.df,
                )
                .write.mode(session.write_mode)
                .parquet(aligned_variant_table_template.format(POP=population))
            )


class LDIndexStep:
    """Build the LD index of each population, from Pan-UKBB and FinnGen.

    Each population is written to its own ``population=<label>`` directory of
    ``ld_index_out`` and replaces only that directory, so one population can be
    rebuilt without the others.

    !!! warning "This step is resource intensive"

        The Pan-UKBB matrices are several TiB each. Build one population per run.
    """

    def __init__(
        self,
        session: Session,
        ld_index_out: str,
        pan_ukbb_aligned_variant_table_template: str,
        pan_ukbb_populations: list[str] = LDIndexConfig().pan_ukbb_populations,
        pan_ukbb_bm_template: str = LDIndexConfig().pan_ukbb_bm_template,
        pan_ukbb_band_blocks: int = LDIndexConfig().pan_ukbb_band_blocks,
        finngen_ld_matrix_path: str | None = LDIndexConfig().finngen_ld_matrix_path,
        min_r2: float = LDIndexConfig().min_r2,
        chromosomes: list[str] | None = LDIndexConfig().chromosomes,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            ld_index_out (str): Output LD index path.
            pan_ukbb_aligned_variant_table_template (str): Aligned variant table of each
                Pan-UKBB population, from ``PanUKBBLDAlignmentStep``, with a ``{POP}`` placeholder.
            pan_ukbb_populations (list[str]): Pan-UKBB populations to build, such as ``EUR``.
            pan_ukbb_bm_template (str): Pan-UKBB LD matrix (hail block matrix) of each
                population, with a ``{POP}`` placeholder.
            pan_ukbb_band_blocks (int): Block-diagonals of the Pan-UKBB matrices read on
                each side of the main diagonal.
            finngen_ld_matrix_path (str | None): FinnGen LD files; None skips FinnGen.
            min_r2 (float): Smallest r2 kept.
            chromosomes (list[str] | None): Build only these chromosomes, such as ``["22"]``,
                reading only their blocks of the Pan-UKBB matrices; None builds all. The
                written population then holds only these chromosomes.
        """
        pan_ukbb = PanUKBBLDMatrix(pan_ukbb_bm_path=pan_ukbb_bm_template)
        for population in pan_ukbb_populations:
            aligned = session.spark.read.parquet(
                pan_ukbb_aligned_variant_table_template.format(POP=population)
            )
            if chromosomes is not None:
                aligned = self._on_chromosomes(aligned, chromosomes)
            self._write(
                pan_ukbb.as_ld_index(
                    population,
                    aligned,
                    min_r2,
                    pan_ukbb_band_blocks,
                    restrict_to_variants=chromosomes is not None,
                ),
                ld_index_out,
            )
            session.logger.info(f"LD index of Pan-UKBB {population} written.")
        if finngen_ld_matrix_path is not None:
            finngen = FinnGenLDMatrix.as_ld_index(
                FinnGenLDMatrix.read(session, finngen_ld_matrix_path), min_r2
            )
            if chromosomes is not None:
                finngen = LDIndex(
                    _df=self._on_chromosomes(finngen.df, chromosomes),
                    _schema=LDIndex.get_schema(),
                )
            self._write(finngen, ld_index_out)
            session.logger.info("LD index of FinnGen written.")

    @staticmethod
    def _on_chromosomes(df: DataFrame, chromosomes: Sequence[str]) -> DataFrame:
        """Keep the rows on the given chromosomes.

        Hydra passes a list option as an OmegaConf ``ListConfig``, which ``isin`` would
        take as a single literal, so it is converted to a list first.

        Args:
            df (DataFrame): Rows with a ``chromosome`` column.
            chromosomes (Sequence[str]): Chromosomes to keep.

        Returns:
            DataFrame: The rows on those chromosomes.
        """
        return df.where(f.col("chromosome").isin(list(chromosomes)))

    @staticmethod
    def _write(ld_index: LDIndex, path: str) -> None:
        """Write one population, replacing whatever the index held for it.

        The population is written to its own ``population=<label>`` directory, which
        Spark reads back as the ``population`` partition column.

        Args:
            ld_index (LDIndex): The LD index of one population.
            path (str): Output LD index path.

        Raises:
            ValueError: If the index does not hold exactly one population.
        """
        populations = [
            row.population
            for row in ld_index.df.select("population").distinct().collect()
        ]
        if len(populations) != 1:
            raise ValueError(f"Expected one population, got {populations}.")
        (
            # Ranges of variants, so a large chromosome is split over several sorted files.
            ld_index.df.drop("population")
            .repartitionByRange("chromosome", "variantId")
            .sortWithinPartitions("variantId", "tagVariantId")
            .write.partitionBy("chromosome")
            .mode("overwrite")
            .parquet(f"{path.rstrip('/')}/population={populations[0]}")
        )
