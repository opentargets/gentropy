"""Tests for gentropy.common.stats."""

from __future__ import annotations

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.common.stats import get_logsum, get_logsum_column


def test_get_logsum_column_matches_get_logsum(spark: SparkSession) -> None:
    """Native column logsumexp must match the numpy get_logsum."""
    arrays = [[0.2, 0.1, 0.05, 0.0], [10.3, 10.5], [1.2, 3.8, 10.2], [-5.0]]
    df = spark.createDataFrame([(i, a) for i, a in enumerate(arrays)], ["id", "arr"])
    rows = df.select("id", get_logsum_column(f.col("arr")).alias("ls")).collect()
    observed = {r["id"]: r["ls"] for r in rows}
    for i, a in enumerate(arrays):
        expected = get_logsum(np.array(a, dtype=np.float64))
        assert np.isclose(observed[i], expected, atol=1e-12), (
            f"mismatch for {a}: {observed[i]} vs {expected}"
        )
