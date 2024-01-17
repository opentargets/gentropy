"""Colocalisation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class Colocalisation(Dataset):
    """Colocalisation results for pairs of overlapping study-locus."""

    @classmethod
    def get_schema(cls: type[Colocalisation]) -> StructType:
        """Provides the schema for the Colocalisation dataset.

        Returns:
            StructType: Schema for the Colocalisation dataset
        """
        return parse_spark_schema("colocalisation.json")
