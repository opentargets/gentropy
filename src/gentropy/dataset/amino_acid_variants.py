"""Dataset representing consequence of amino-acid changes in protein."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class AminoAcidVariants(Dataset):
    """Dataset representing consequence of amino-acid changes in protein."""

    @classmethod
    def get_schema(cls: type[AminoAcidVariants]) -> StructType:
        """Provides the schema for the AminoAcidVariants dataset.

        Returns:
            StructType: Schema for the AminoAcidVariants dataset
        """
        return parse_spark_schema("amino_acid_variants.json")
