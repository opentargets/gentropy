"""Interval dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
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
        biosample_index: BiosampleIndex,
        biosample_mapping: DataFrame,
    ) -> Intervals:
        """Collect interval data for a particular source.

        Args:
            spark (SparkSession): Spark session
            source_name (str): Name of the interval source
            source_path (str): Path to the interval source file
            target_index (TargetIndex): Target index
            biosample_index (BiosampleIndex): Biosample index
            biosample_mapping (DataFrame): Biosample mapping DataFrame

        Returns:
            Intervals: Intervals dataset

        Raises:
            ValueError: If the source name is not recognised
        """
        from gentropy.datasource.intervals.e2g import IntervalsE2G
        from gentropy.datasource.intervals.epiraction import IntervalsEpiraction

        if source_name == "e2g":
            raw = IntervalsE2G.read(spark, source_path)
            return IntervalsE2G.parse(
                raw_e2g_df=raw,
                biosample_mapping=biosample_mapping,
                target_index=target_index,
                biosample_index=biosample_index,
            )

        if source_name == "epiraction":
            raw = IntervalsEpiraction.read(spark, source_path)
            return IntervalsEpiraction.parse(
                raw_epiraction_df=raw,
                target_index=target_index,
            )

        raise ValueError(f"Unknown interval source: {source_name!r}")
