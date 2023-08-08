"""Gene index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


@dataclass
class GeneIndex(Dataset):
    """Gene index dataset.

    Gene-based annotation.
    """

    @staticmethod
    def _get_gene_tss(strand_col: Column, start_col: Column, end_col: Column) -> Column:
        """Returns the TSS of a gene based on its orientation.

        Args:
            strand_col (Column): Column containing 1 if the coding strand of the gene is forward, and -1 if it is reverse.
            start_col (Column): Column containing the start position of the gene.
            end_col (Column): Column containing the end position of the gene.

        Returns:
            Column: Column containing the TSS of the gene.

        Examples:
            >>> df = spark.createDataFrame([{"strand": 1, "start": 100, "end": 200}, {"strand": -1, "start": 100, "end": 200}])
            >>> df.withColumn("tss", GeneIndex._get_gene_tss(f.col("strand"), f.col("start"), f.col("end"))).show()
            +---+-----+------+---+
            |end|start|strand|tss|
            +---+-----+------+---+
            |200|  100|     1|100|
            |200|  100|    -1|200|
            +---+-----+------+---+
            <BLANKLINE>

        """
        return f.when(strand_col == 1, start_col).when(strand_col == -1, end_col)

    @classmethod
    def _get_schema(cls: type[GeneIndex]) -> StructType:
        """Provides the schema for the GeneIndex dataset."""
        return parse_spark_schema("targets.json")

    @classmethod
    def from_source(cls: type[GeneIndex], target_index: DataFrame) -> GeneIndex:
        """Initialise GeneIndex from source dataset.

        Args:
            target_index (DataFrame): Target index dataframe

        Returns:
            GeneIndex: Gene index dataset
        """
        return cls(
            _df=target_index.select(
                f.coalesce(f.col("id"), f.lit("unknown")).alias("geneId"),
                f.coalesce(f.col("genomicLocation.chromosome"), f.lit("unknown")).alias(
                    "chromosome"
                ),
                GeneIndex._get_gene_tss(
                    f.col("genomicLocation.strand"),
                    f.col("genomicLocation.start"),
                    f.col("genomicLocation.end"),
                ).alias("tss"),
                "biotype",
                "approvedSymbol",
                "obsoleteSymbols",
            ),
            _schema=cls._get_schema(),
        )

    def filter_by_biotypes(self: GeneIndex, biotypes: list) -> GeneIndex:
        """Filter by approved biotypes.

        Args:
            biotypes (list): List of Ensembl biotypes to keep.

        Returns:
            GeneIndex: Gene index dataset filtered by biotypes.
        """
        self.df = self._df.filter(f.col("biotype").isin(biotypes))
        return self

    def locations_lut(self: GeneIndex) -> DataFrame:
        """Gene location information.

        Returns:
            DataFrame: Gene LUT including genomic location information.
        """
        return self.df.select(
            "geneId",
            "chromosome",
            "tss",
        )

    def symbols_lut(self: GeneIndex) -> DataFrame:
        """Gene symbol lookup table.

        Pre-processess gene/target dataset to create lookup table of gene symbols, including
        obsoleted gene symbols.

        Returns:
            DataFrame: Gene LUT for symbol mapping containing `geneId` and `geneSymbol` columns.
        """
        return self.df.select(
            "geneId",
            f.explode(
                f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label"))
            ).alias("geneSymbol"),
        )
