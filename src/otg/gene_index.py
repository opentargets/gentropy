"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from otg.config import GeneIndexStepConfig
from otg.dataset.gene_index import GeneIndex

if TYPE_CHECKING:
    from otg.common.session import Session


@dataclass
class GeneIndexStep(GeneIndexStepConfig):
    """Gene index step.

    This step generates a gene index dataset from an Open Targets Platform target dataset.
    """

    session: Session = SparkSession.builder.getOrCreate()

    def run(self: GeneIndexStepConfig) -> None:
        """Run Target index step."""
        # Extract
        platform_target = self.session.spark.read.parquet(self.target_path)
        # Transform
        gene_index = GeneIndex.from_source(platform_target)
        # Load
        gene_index.df.write.mode(self.session.write_mode).parquet(self.gene_index_path)
