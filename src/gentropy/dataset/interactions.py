"""Gene-gene interactions dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class Interactions(Dataset):
    """Gene-gene interactions from the Open Targets Platform ``interaction`` output.

    Only the columns used by gentropy are read: the source database, the two interacting
    targets and the interaction score.
    """

    @classmethod
    def get_schema(cls: type[Interactions]) -> StructType:
        """Provides the schema for the Interactions dataset.

        Returns:
            StructType: Schema for the Interactions dataset.
        """
        return parse_spark_schema("interactions.json")

    def high_confidence(
        self: Interactions, source_database: str, min_score: float
    ) -> Interactions:
        """Keep the interactions of one source scoring at least ``min_score``.

        Args:
            source_database (str): ``sourceDatabase`` to keep, e.g. ``string``.
            min_score (float): Minimum interaction ``scoring``.

        Returns:
            Interactions: The retained interactions.

        Examples:
            >>> df = spark.createDataFrame(
            ...     [("string", "g1", "g2", 0.9), ("string", "g1", "g3", 0.5), ("intact", "g1", "g4", 0.9)],
            ...     "sourceDatabase string, targetA string, targetB string, scoring double",
            ... )
            >>> Interactions(_df=df).high_confidence("string", 0.75).df.select("targetA", "targetB").show()
            +-------+-------+
            |targetA|targetB|
            +-------+-------+
            |     g1|     g2|
            +-------+-------+
            <BLANKLINE>
        """
        return self.filter(
            (f.col("sourceDatabase") == source_database)
            & (f.col("scoring") >= min_score)
        )
