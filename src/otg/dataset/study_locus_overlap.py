"""Study locus overlap index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class StudyLocusOverlap(Dataset):
    """Study-Locus overlap.

    This dataset captures pairs of overlapping `StudyLocus`.
    """

    _schema: StructType = parse_spark_schema("study_locus_overlap.json")

    @classmethod
    def from_parquet(
        cls: type[StudyLocusOverlap], session: Session, path: str
    ) -> StudyLocusOverlap:
        """Initialise StudyLocusOverlap from parquet file.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file

        Returns:
            StudyLocusOverlap: Study-locus overlap dataset
        """
        return super().from_parquet(session, path, cls._schema)
