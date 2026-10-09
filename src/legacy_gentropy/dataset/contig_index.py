"""Contig (chromosome) index."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class ContigIndex(Dataset):
    """Contig index.

    A contig index captures contiguous data structure with `id`, `start`, `end` fields.
    This dataset can represent chromosome bounds, contigs or scaffolds.

    The indexing is expected to be 0-based.

    Examples:
    ---
    >>> df = spark.createDataFrame([
    ...     ("1", 0, 248956422),
    ...     ("2", 0, 242193529),
    ...    ("X", 0, 156040895),],
    ...    schema=["id", "start", "end"])
    >>> contig_index = ContigIndex(_df=df)
    >>> contig_index.canonical().df.show()
    +---+-----+---------+
    | id|start|      end|
    +---+-----+---------+
    |  1|    0|248956422|
    |  2|    0|242193529|
    |  X|    0|156040895|
    +---+-----+---------+
    <BLANKLINE>
    """

    CANONICAL_CHROMOSOMES = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
    """Canonical chromosomes"""

    @classmethod
    def get_schema(cls: type[ContigIndex]) -> StructType:
        """Provide the schema for the ContigIndex dataset.

        Returns:
            StructType: The schema of the ContigIndex dataset.
        """
        return parse_spark_schema("contig_index.json")

    def canonical(self) -> ContigIndex:
        """Get the canonical subpart of the index.

        Returns:
            ContigIndex: Filtered by canonical chromosomes.
        """
        return ContigIndex(
            _df=self.df.filter(f.col("id").isin(self.CANONICAL_CHROMOSOMES))
        )
