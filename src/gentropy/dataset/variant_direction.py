"""Dataset representing direct and flipped variants."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class VariantDirection(Dataset):
    """Dataset representing direct and flipped variants.

    This dataset contains the original and flipped variant representations,
    which are used for harmonising summary statistics across different datasets.
    """

    @classmethod
    def get_schema(cls: type[VariantDirection]) -> StructType:
        """Provides the schema for the VariantDirection dataset.

        Returns:
            StructType: Schema for the VariantDirection dataset
        """
        return parse_spark_schema("variant_direction.json")
