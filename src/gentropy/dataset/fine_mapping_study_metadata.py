"""Fine-mapping study metadata dataset."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.session import Session
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


class FineMappingStudyMetadataRecord(BaseModel):
    """One study metadata record used to construct fine-mapping inputs."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    study_id: str = Field(alias="studyId", min_length=1)
    ancestry: str = Field(min_length=1)
    sample_size: StrictInt = Field(alias="sampleSize")

    @classmethod
    def from_jsonl(
        cls, path: str | Path
    ) -> list[FineMappingStudyMetadataRecord]:
        """Read and validate one metadata record per JSONL line.

        Args:
            path (str | Path): Path to a JSONL file containing study metadata records.

        Returns:
            list[FineMappingStudyMetadataRecord]: Validated metadata records in file order.

        Raises:
            ValueError: If a non-empty line is not valid JSON or does not match
                the metadata record schema.
        """
        records: list[FineMappingStudyMetadataRecord] = []
        study_ids: set[str] = set()
        for line_number, line in enumerate(Path(path).read_text().splitlines(), 1):
            if not line.strip():
                continue
            try:
                record = cls.model_validate_json(line)
            except (ValueError, json.JSONDecodeError) as error:
                raise ValueError(
                    f"Invalid fine-mapping metadata at line {line_number}"
                ) from error
            if record.study_id in study_ids:
                raise ValueError(f"Duplicate studyId: {record.study_id}")
            study_ids.add(record.study_id)
            records.append(record)
        return records


@dataclass
class FineMappingStudyMetadata(Dataset):
    """Study-level metadata required to construct fine-mapping inputs."""

    @classmethod
    def from_jsonl(
        cls: type[FineMappingStudyMetadata], session: Session, path: str | Path
    ) -> FineMappingStudyMetadata:
        """Build the metadata dataset from validated JSONL records.

        Args:
            session (Session): Gentropy session used to create the Spark dataframe.
            path (str | Path): Path to a JSONL file containing study metadata records.

        Returns:
            FineMappingStudyMetadata: Validated metadata dataset.
        """
        records = FineMappingStudyMetadataRecord.from_jsonl(path)
        dataframe = session.spark.createDataFrame(
            [record.model_dump(by_alias=True) for record in records],
            schema=cls.get_schema(),
        )
        return cls(_df=dataframe, _schema=cls.get_schema())

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
