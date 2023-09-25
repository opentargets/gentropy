"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class StudyIndex(Dataset):
    """Study index dataset.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    @classmethod
    def get_schema(cls: type[StudyIndex]) -> StructType:
        """Provides the schema for the StudyIndex dataset."""
        return parse_spark_schema("studies.json")

    def study_type_lut(self: StudyIndex) -> DataFrame:
        """Return a lookup table of study type.

        Returns:
            DataFrame: A dataframe containing `studyId` and `studyType` columns.
        """
        return self.df.select("studyId", "studyType")
