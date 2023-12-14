"""Test Open Targets target data source."""
from __future__ import annotations

from otg.dataset.gene_index import GeneIndex
from otg.datasource.open_targets.target import OpenTargetsTarget
from pyspark.sql import DataFrame


def test_open_targets_as_gene_index(sample_target_index: DataFrame) -> None:
    """Test gene index from source."""
    assert isinstance(OpenTargetsTarget.as_gene_index(sample_target_index), GeneIndex)
