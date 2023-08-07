"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class StudyLocusOverlap(Dataset):
    """Study-Locus overlap.

    This dataset captures pairs of overlapping `StudyLocus`.
    """

    @classmethod
    def _get_schema(cls: type[StudyLocusOverlap]) -> StructType:
        """Provides the schema for the StudyLocusOverlap dataset."""
        return parse_spark_schema("study_locus_overlap.json")
