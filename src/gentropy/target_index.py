"""Step to generate gene index dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.open_targets.target import OpenTargetsTarget


class GeneIndexStep:
    """Gene index step.

    This step generates a gene index dataset from an Open Targets Platform target dataset.
    """

    def __init__(
        self,
        session: Session,
        target_path: str,
        gene_index_path: str,
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            target_path (str): Input Open Targets Platform target dataset path.
            gene_index_path (str): Output gene index dataset path.
        """
        platform_target = session.spark.read.parquet(target_path)
        # Transform
        gene_index = OpenTargetsTarget.as_gene_index(platform_target)
        # Load
        gene_index.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(gene_index_path)
