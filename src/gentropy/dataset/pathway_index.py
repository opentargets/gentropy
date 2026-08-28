"""Pathway index dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.common.session import Session


@dataclass
class PathwayIndex(Dataset):
    """Gene set membership of a pathway library.

    One row per pathway, holding the gene symbols that make up the gene set. This is the
    reference that disease-pathway enrichment results are computed against, and it is what
    turns an enriched pathway back into a set of genes.
    """

    SOURCE_PATTERN = r"\[([^\]]+)\]\s*$"
    """Pattern extracting the library a pathway came from out of its name, e.g. `[Reactome]`."""

    @classmethod
    def get_schema(cls: type[PathwayIndex]) -> StructType:
        """Provide the schema for the PathwayIndex dataset.

        Returns:
            StructType: The schema of the PathwayIndex dataset.
        """
        return parse_spark_schema("pathway_index.json")

    @classmethod
    def from_gmt(cls: type[PathwayIndex], session: Session, path: str) -> PathwayIndex:
        """Parse a gene matrix transposed (GMT) file into a PathwayIndex.

        A GMT file holds one tab separated gene set per line: the pathway name, a
        description that is ignored here, and then the gene symbols.

        Args:
            session (Session): Spark session
            path (str): Path to the GMT file

        Returns:
            PathwayIndex: Pathway index dataset
        """
        columns = f.split(f.col("value"), "\t")
        pathway = columns.getItem(0)
        return cls(
            _df=(
                session.spark.read.text(path)
                .filter(f.trim(f.col("value")) != "")
                .select(
                    pathway.alias("pathway"),
                    f.regexp_extract(pathway, cls.SOURCE_PATTERN, 1).alias("source"),
                    f.array_distinct(
                        f.filter(
                            f.slice(columns, 3, f.size(columns)), lambda gene: gene != ""
                        )
                    ).alias("geneSymbols"),
                )
                .filter(f.size("geneSymbols") > 0)
            ),
            _schema=cls.get_schema(),
        )

    def gene_membership(self: PathwayIndex) -> DataFrame:
        """Explode the gene sets into one row per pathway and gene symbol.

        Returns:
            DataFrame: Dataframe with `pathway` and `geneSymbol` columns.
        """
        return self.df.select(
            "pathway", f.explode("geneSymbols").alias("geneSymbol")
        ).distinct()
