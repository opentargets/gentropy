"""Fine-mapping study metadata dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class FineMappingStudyMetadata(Dataset):
    """Study-level metadata required to construct fine-mapping inputs."""

    def __post_init__(self: FineMappingStudyMetadata) -> None:
        """Validate that each study has exactly one metadata row."""
        duplicate_studies = (
            self.df.groupBy("studyId").count().filter("count > 1").limit(1).count()
        )
        assert duplicate_studies == 0, "FineMappingStudyMetadata must contain one row per studyId"
        super().__post_init__()

    @classmethod
    def get_schema(cls: type[FineMappingStudyMetadata]) -> StructType:
        """Provide the schema for the dataset.

        Returns:
            StructType: Schema for fine-mapping study metadata.
        """
        return parse_spark_schema("fine_mapping_study_metadata.json")
