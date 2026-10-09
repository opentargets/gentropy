"""Target index dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql import types as t

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class TargetIndex(Dataset):
    """Target index dataset.

    Gene-based annotation.
    """

    @classmethod
    def get_schema(cls: type[TargetIndex]) -> StructType:
        """Provides the schema for the TargetIndex dataset.

        Returns:
            StructType: Schema for the TargetIndex dataset
        """
        return parse_spark_schema("target_index.json")

    def filter_by_biotypes(self: TargetIndex, biotypes: list[str]) -> TargetIndex:
        """Filter by approved biotypes.

        Args:
            biotypes (list[str]): List of Ensembl biotypes to keep.

        Returns:
            TargetIndex: Target index dataset filtered by biotypes.
        """
        self.df = self._df.filter(f.col("biotype").isin(biotypes))
        return self

    def locations_lut(self: TargetIndex) -> DataFrame:
        """Gene location information.

        Returns:
            DataFrame: Gene LUT including genomic location information.
        """
        return self.df.select(
            f.col("id").alias("geneId"),
            f.col("genomicLocation.chromosome").alias("chromosome"),
            f.col("genomicLocation.start").alias("start"),
            f.col("genomicLocation.end").alias("end"),
            f.col("genomicLocation.strand").alias("strand"),
            "tss",
        )

    def symbols_lut(self: TargetIndex) -> DataFrame:
        """Gene symbol lookup table.

        Pre-processess gene/target dataset to create lookup table of gene symbols, including
        obsoleted gene symbols.

        Returns:
            DataFrame: Gene LUT for symbol mapping containing `geneId` and `geneSymbol` columns.
        """
        return self.df.select(
            f.explode(
                f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label"))
            ).alias("geneSymbol"),
            f.col("id").alias("geneId"),
            f.col("genomicLocation.chromosome").alias("chromosome"),
            "tss",
        )

    def protein_id_lut(
        self: TargetIndex,
        include_par_chr: str = "X",
    ) -> DataFrame:
        """Mapping between gene id and protein id.

        Args:
            include_par_chr (str): Chromosome to include PAR (pseudo-autosomal region)
                genes from. Must be ``"X"`` or ``"Y"``. Defaults to ``"X"``.

        Returns:
            DataFrame: Gene LUT for UniProt mapping containing `geneId`, `proteinId` columns.

        Raises:
            ValueError: If ``include_par_chr`` is not ``"X"`` or ``"Y"``.

        !!! note "PAR gene mapping"
            For the PAR (pseudo autosomal region) genes we want to keep only one mapping by default for chromosome X
            to avoid the confusion of having the same geneSymbol mapped to two ensembl ids.

        """
        if include_par_chr not in ("X", "Y"):
            raise ValueError("include_par_chr must be either 'X' or 'Y'")

        # Condition checks if there are multiple chromosomes for the same proteinId, example: ASMTL gene
        # [ENSG00000169093, ENSG00000292339] mapped to X and Y due to PAR region.
        is_par = f.concat_ws(
            ",",
            f.sort_array(
                f.collect_set("chromosome").over(Window.partitionBy("proteinId"))
            ),
        ) == f.lit("X,Y")

        _df = (
            self.df.select(
                f.col("id").alias("geneId"),
                f.inline("proteinIds"),
                f.col("canonicalTranscript.chromosome").alias("chromosome"),
            )
            .withColumnRenamed("id", "proteinId")
            .select(
                f.col("geneId"),
                f.col("proteinId"),
                is_par.alias("isPAR"),
                f.col("chromosome"),
            )
        )
        if include_par_chr:
            _df = _df.filter(
                ~((f.col("isPAR")) & (~f.col("chromosome").isin([include_par_chr])))
            ).drop("isPAR", "chromosome")

        return _df

    def tss_lut(self: TargetIndex) -> DataFrame:
        """Gene TSS lookup table.

        The TSS is determined using the following priority:
        1. preferred TSS from target index
        2. canonical transcript start|end based on strand
        3. genomic location start|end based on strand

        Returns:
            DataFrame: Gene LUT for TSS mapping containing `geneId` and `tss` columns.
        """
        ct_tss = f.when(
            f.col("canonicalTranscript.strand") == "+",
            f.col("canonicalTranscript.start"),
        ).when(
            f.col("canonicalTranscript.strand") == "-",
            f.col("canonicalTranscript.end"),
        )
        gl_tss = f.when(
            f.col("genomicLocation.strand") == 1, f.col("genomicLocation.start")
        ).when(f.col("genomicLocation.strand") == -1, f.col("genomicLocation.end"))

        tss = f.coalesce(f.col("tss"), ct_tss, gl_tss).cast(t.LongType()).alias("tss")
        return self.df.select(f.col("id").alias("geneId"), tss)
