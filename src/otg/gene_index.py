"""Step to generate gene index dataset."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.open_targets.target import OpenTargetsTarget


@dataclass
class GeneIndexStep:
    """Gene index step.

    This step generates a gene index dataset from an Open Targets Platform target dataset.

    Attributes:
        session (Session): Session object.
        target_path (str): Open targets Platform target dataset path.
        gene_index_path (str): Output gene index path.
    """

    session: Session

    target_path: str = MISSING
    gene_index_path: str = MISSING

    def __post_init__(self: GeneIndexStep) -> None:
        """Run step."""
        # Extract
        platform_target = self.session.spark.read.parquet(self.target_path)
        # Transform
        gene_index = OpenTargetsTarget.as_gene_index(platform_target)
        # Load
        gene_index.df.write.mode(self.session.write_mode).parquet(self.gene_index_path)
