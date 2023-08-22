"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import Session
    from otg.dataset.study_index import StudyIndex
    from otg.dataset.study_locus import StudyLocus


@dataclass
class StudyLocusOverlap(Dataset):
    """Study-Locus overlap.

    This dataset captures pairs of overlapping `StudyLocus`: that is associations whose credible sets share at least one tagging variant.

    This is a helpful dataset for other downstream analyses, such as colocalisation.

    !!! note
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
        df = session.read_parquet(path=path, schema=cls._schema)
        return cls(_df=df, _schema=cls._schema)

    @classmethod
    def from_associations(
        cls: type[StudyLocusOverlap], study_locus: StudyLocus, study_index: StudyIndex
    ) -> StudyLocusOverlap:
        """Initialise StudyLocusOverlap from associations.

        Args:
            study_locus (StudyLocus): Study-locus associations to find the overlapping signals
            study_index (StudyIndex): Study index to find the overlapping signals

        Returns:
            StudyLocusOverlap: Study-locus overlap dataset
        """
        return study_locus.find_overlaps(study_index)
