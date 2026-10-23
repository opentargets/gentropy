"""Interval evidence used by downstream locus-to-gene features."""

from dataclasses import dataclass

from pyspark.sql.types import StructType

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset


@dataclass
class Intervals(Dataset):
    """Dataset linking genes to genomic regions."""

    @classmethod
    def get_schema(cls) -> StructType:
        """Provides the schema for the Intervals dataset.

        Returns:
            StructType: Schema for the Intervals dataset
        """
        return parse_spark_schema("intervals.json")
