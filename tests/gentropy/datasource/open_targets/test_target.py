"""Test Open Targets target data source."""

from __future__ import annotations

from pyspark.sql import DataFrame

from gentropy.dataset.gene_index import GeneIndex
from gentropy.datasource.open_targets.target import OpenTargetsTarget


def test_open_targets_as_gene_index(sample_target_index: DataFrame) -> None:
    """Test gene index from source."""
    assert isinstance(OpenTargetsTarget.as_gene_index(sample_target_index), GeneIndex)
