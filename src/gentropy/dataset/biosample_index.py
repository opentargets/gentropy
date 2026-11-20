"""Biosample index dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class BiosampleIndex(Dataset):
    """Biosample index dataset.

    A Biosample index dataset captures the metadata of the biosamples (e.g. tissues, cell types, cell lines, etc) such as alternate names and relationships with other biosamples.
    """

    @classmethod
    def get_schema(cls: type[BiosampleIndex]) -> StructType:
        """Provide the schema for the BiosampleIndex dataset.

        Returns:
            StructType: The schema of the BiosampleIndex dataset.
        """
        return parse_spark_schema("biosample_index.json")
