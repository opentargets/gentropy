"""Summary statistics QC dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class SummaryStatisticsQC(Dataset):
    """Summary Statistics Quality Controls dataset."""

    @classmethod
    def get_schema(cls: type[SummaryStatisticsQC]) -> StructType:
        """Provide the schema for the SummaryStatisticsQC dataset."""
        return parse_spark_schema("summary_statistics_qc.json")
