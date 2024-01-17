"""V2G dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.gene_index import GeneIndex


@dataclass
class V2G(Dataset):
    """Variant-to-gene (V2G) evidence dataset.

    A variant-to-gene (V2G) evidence is understood as any piece of evidence that supports the association of a variant with a likely causal gene. The evidence can sometimes be context-specific and refer to specific `biofeatures` (e.g. cell types)
    """

    @classmethod
    def get_schema(cls: type[V2G]) -> StructType:
        """Provides the schema for the V2G dataset.

        Returns:
            StructType: Schema for the V2G dataset
        """
        return parse_spark_schema("v2g.json")

    def filter_by_genes(self: V2G, genes: GeneIndex) -> V2G:
        """Filter V2G dataset by genes.

        Args:
            genes (GeneIndex): Gene index dataset to filter by

        Returns:
            V2G: V2G dataset filtered by genes
        """
        self.df = self._df.join(genes.df.select("geneId"), on="geneId", how="inner")
        return self

    def extract_distance_tss_minimum(self: V2G) -> None:
        """Extract minimum distance to TSS."""
        self.df = self._df.filter(f.col("distance")).withColumn(
            "distanceTssMinimum",
            f.expr("min(distTss) OVER (PARTITION BY studyLocusId)"),
        )
