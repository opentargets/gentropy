"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class Colocalisation(Dataset):
    """Colocalisation results for pairs of overlapping study-locus."""

    schema: StructType = parse_spark_schema("colocalisation.json")

    @classmethod
    def from_parquet(
        cls: Type[Colocalisation], etl: ETLSession, path: str
    ) -> Colocalisation:
        """Initialise Colocalisation dataset from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            Colocalisation: Colocalisation results
        """
        return super().from_parquet(etl, path, cls.schema)
