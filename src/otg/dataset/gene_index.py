"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class GeneIndex(Dataset):
    """Gene index dataset.

    Gene-based annotation.
    """

    schema: StructType = parse_spark_schema("targets.json")

    @staticmethod
    def _get_gene_tss(strand_col: Column, start_col: Column, end_col: Column) -> Column:
        """Returns the TSS of a gene based on its orientation.

        Args:
            strand_col (Column): Column containing 1 if the coding strand of the gene is forward, and -1 if it is reverse.
            start_col (Column): Column containing the start position of the gene.
            end_col (Column): Column containing the end position of the gene.

        Returns:
            Column: Column containing the TSS of the gene.
        """
        return f.when(strand_col == 1, start_col).when(strand_col == -1, end_col)

    @classmethod
    def from_parquet(cls: Type[GeneIndex], etl: ETLSession, path: str) -> GeneIndex:
        """Initialise GeneIndex from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            GeneIndex: Gene index dataset
        """
        return super().from_parquet(etl, path, cls.schema)

    def filter_by_biotypes(self: GeneIndex, biotypes: list) -> None:
        """Filter by approved biotypes.

        Args:
            biotypes (list): List of Ensembl biotypes to keep.
        """
        self.df = self._df.filter(f.col("biotype").isin(biotypes))

    def locations_lut(self: GeneIndex) -> DataFrame:
        """Gene location information.

        Returns:
            DataFrame: Gene LUT including genomic location information.
        """
        return self.df.select(
            f.col("id").alias("geneId"),
            f.col("genomicLocation.chromosome").alias("chromosome"),
            self._get_gene_tss(
                f.col("genomicLocation.strand"),
                f.col("genomicLocation.start"),
                f.col("genomicLocation.end"),
            ).alias("tss"),
            "genomicLocation",
        )

    def symbols_lut(self: GeneIndex) -> DataFrame:
        """Gene symbol lookup table.

        Pre-processess gene/target dataset to create lookup table of gene symbols, including
        obsoleted gene symbols.

        Returns:
            DataFrame: Gene LUT for symbol mapping containing `geneId` and `geneSymbol` columns.
        """
        return self.df.select(
            f.col("id").alias("geneId"),
            f.explode(
                f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label"))
            ).alias("geneSymbol"),
        )
