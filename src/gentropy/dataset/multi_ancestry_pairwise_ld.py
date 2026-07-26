"""Multi-ancestry pairwise LD dataset."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isqrt
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.pairwise_ld import PairwiseLD

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class MultiAncestryPairwiseLD(Dataset):
    """Pairwise LD matrices for multiple ancestries in one dataset."""

    dimensions: dict[str, tuple[int, int]] = field(init=False)

    def __post_init__(self: MultiAncestryPairwiseLD) -> None:
        """Validate one square pairwise matrix for every ancestry."""
        row_counts = self.df.groupBy("ancestry").count().collect()
        self.dimensions = {}
        for row in row_counts:
            row_count = row["count"]
            dimension = isqrt(row_count)
            assert dimension * dimension == row_count, (
                "The number of rows in a multi-ancestry pairwise LD table has to "
                f"be square for each ancestry. Ancestry {row['ancestry']} has "
                f"{row_count} rows."
            )
            self.dimensions[row["ancestry"]] = (dimension, dimension)
        super().__post_init__()

    @classmethod
    def get_schema(cls: type[MultiAncestryPairwiseLD]) -> StructType:
        """Provide the schema for the dataset.

        Returns:
            StructType: Schema for the multi-ancestry pairwise LD dataset.
        """
        return parse_spark_schema("multi_ancestry_pairwise_ld.json")

    def for_ancestry(self: MultiAncestryPairwiseLD, ancestry: str) -> PairwiseLD:
        """Return one ancestry-specific matrix using the PairwiseLD contract.

        Args:
            ancestry (str): Ancestry to project into a PairwiseLD dataset.

        Returns:
            PairwiseLD: Ancestry-specific pairwise LD dataset.
        """
        if ancestry not in self.dimensions:
            raise ValueError(f"Unknown ancestry: {ancestry}")
        return PairwiseLD(
            _df=self.df.filter(f.col("ancestry") == ancestry).drop("ancestry"),
            _schema=PairwiseLD.get_schema(),
        )

    def ancestries(self: MultiAncestryPairwiseLD) -> list[str]:
        """Return ancestry labels in deterministic order.

        Returns:
            list[str]: Sorted ancestry labels.
        """
        return sorted(self.dimensions)
