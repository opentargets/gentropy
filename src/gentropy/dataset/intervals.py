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
        """Return the interval evidence schema."""
        return parse_spark_schema("intervals.json")
