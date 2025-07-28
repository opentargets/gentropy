"""Interval dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    @classmethod
    def get_schema(cls: type[Intervals]) -> StructType:
        """Provides the schema for the Intervals dataset.

        Returns:
            StructType: Schema for the Intervals dataset
        """
        return parse_spark_schema("intervals.json")

    @classmethod
    def from_source(
        cls: type[Intervals],
        spark: SparkSession,
        source_name: str,
        source_path: str,
        target_index: TargetIndex,
    ) -> Intervals:
        """Collect interval data for a particular source.

        Args:
            spark (SparkSession): Spark session
            source_name (str): Name of the interval source
            source_path (str): Path to the interval source file
            target_index (TargetIndex): Target index

        Returns:
            Intervals: Intervals dataset

        Raises:
            ValueError: If the source name is not recognised
        """
        from gentropy.datasource.intervals.e2g import IntervalsE2G
        from gentropy.datasource.intervals.epiraction import IntervalsEpiraction

        source_to_class = {
            "epiraction": IntervalsEpiraction,
            "e2g": IntervalsE2G,
        }

        if source_name not in source_to_class:
            raise ValueError(f"Unknown interval source: {source_name}")

        source_class = source_to_class[source_name]
        data = source_class.read(spark, source_path)  # type: ignore
        return source_class.parse(data, target_index)  # type: ignore
