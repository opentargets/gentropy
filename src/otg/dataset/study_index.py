"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class StudyIndex(Dataset):
    """Study index dataset."""

    schema: StructType = parse_spark_schema("studies.json")

    @classmethod
    def from_parquet(cls: Type[StudyIndex], etl: ETLSession, path: str) -> StudyIndex:
        """Initialise StudyIndex from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            StudyIndex: Study index dataset
        """
        return super().from_parquet(etl, path, cls.schema)

    def study_type_lut(self: StudyIndex) -> DataFrame:
        """Return a lookup table of study type.

        Returns:
            DataFrame: A dataframe containing `studyId` and `studyType` columns.
        """
        return self.df.select("studyId", "studyType")
