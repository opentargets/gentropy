"""Pairwise LD dataset."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import sqrt
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class PairwiseLD(Dataset):
    """Pairwise variant correlation dataset.

    This class captures logic applied on pairwise linkage data + by validation ensuring data quality.
    """

    dimension: tuple[int, int] = field(init=False)

    def __post_init__(self: PairwiseLD) -> None:
        """Validating the dataset upon creation.

        - Besides the schema, a pairwise LD table is expected have rows being a square number.

        Raises:
            AssertionError: When the number of rows in the provided dataframe to construct the LD matrix is not even after applying square root.
        """
        row_count = self.df.count()

        assert int(sqrt(row_count)) == sqrt(row_count), (
            f"The number of rows in a pairwise LD table has to be square. Got: {row_count}"
        )

        self.dimension = (int(sqrt(row_count)), int(sqrt(row_count)))
        super().__post_init__()

    @classmethod
    def get_schema(cls: type[PairwiseLD]) -> StructType:
        """Provide the schema for the dataset.

        Returns:
            StructType: The schema of the dataset.
        """
        return parse_spark_schema("pairwise_ld.json")
