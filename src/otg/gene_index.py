"""Step to generate gene index dataset."""
from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import GeneIndexStepConfig
from otg.dataset.gene_index import GeneIndex


@dataclass
class GeneIndexStep(GeneIndexStepConfig):
    """Gene index step.

    This step generates a gene index dataset from an Open Targets Platform target dataset.
    """

    session: Session = Session()

    def run(self: GeneIndexStep) -> None:
        """Run Target index step."""
        # Extract
        platform_target = self.session.spark.read.parquet(self.target_path)
        # Transform
        gene_index = GeneIndex.from_source(platform_target)
        # Load
        gene_index.df.write.mode(self.session.write_mode).parquet(self.gene_index_path)
