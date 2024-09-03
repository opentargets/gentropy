"""L2G Feature Dataset."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class L2GFeature(Dataset, ABC):
    """Locus-to-gene feature dataset."""

    @classmethod
    def get_schema(cls: type[L2GFeature]) -> StructType:
        """Provides the schema for the L2GFeature dataset.

        Returns:
            StructType: Schema for the L2GFeature dataset
        """
        return parse_spark_schema("l2g_feature.json")

    @classmethod
    @abstractmethod
    def compute(cls: type[L2GFeature]) -> L2GFeature:
        """Computes the L2GFeature dataset.

        Returns:
            L2GFeature: a L2GFeature dataset
        """
        pass
