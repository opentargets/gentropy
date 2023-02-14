"""V2G dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession
    from otg.dataset.gene_index import GeneIndex


@dataclass
class V2G(Dataset):
    """Variant-to-gene (V2G) evidence dataset.

    A variant-to-gene (V2G) evidence is understood as any piece of evidence that supports the association of a variant with a likely causal gene. The evidence can sometimes be context-specific and refer to specific `biofeatures` (e.g. cell types)
    """

    schema: StructType = parse_spark_schema("v2g.json")

    @classmethod
    def from_parquet(cls: type[V2G], etl: ETLSession, path: str) -> V2G:
        """Initialise V2G from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            V2G: V2G dataset
        """
        return super().from_parquet(etl, path, cls.schema)

    def filter_by_genes(self: V2G, genes: GeneIndex) -> None:
        """Filter by V2G dataset by genes.

        Args:
            genes (GeneIndex): Gene index dataset to filter by
        """
        self.df = self._df.join(
            genes.df.select(f.col("id").alias("geneId")), on="geneId", how="inner"
        )

    def extract_distance_tss_minimum(self: V2G) -> None:
        """Extract minimum distance to TSS."""
        self.df = self._df.filter(f.col("distance")).withColumn(
            "distanceTssMinimum",
            f.expr("min(distTss) OVER (PARTITION BY studyLocusId)"),
        )
