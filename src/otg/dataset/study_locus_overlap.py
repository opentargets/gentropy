"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import Session
    from otg.dataset.study_index import StudyIndex
    from otg.dataset.study_locus import StudyLocus


class StudyLocusOverlapMethod(Enum):
    """Applied method to find overlaps between StudyLocus associations.

    Attributes:
        LD (str): Find overlaps between StudyLocus associations using LD information. This method will find overlapping signals in associations where credible sets share at least one tagging variant.
        DISTANCE (str): Find overlaps between StudyLocus associations within a given region. This method will find overlapping signals in associations where variants are common to both regions and are within a given distance.
    """

    LD = "LD"
    DISTANCE = "DISTANCE"


@dataclass
class StudyLocusOverlap(Dataset):
    """Study-Locus overlap.

    This dataset captures pairs of overlapping `StudyLocus`. We define two types of overlaps:
    - associations whose credible sets share at least one tagging variant;
    - associations whose variants are common to both regions and are within a given distance.
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
        cls: type[StudyLocusOverlap],
        method: StudyLocusOverlapMethod,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        distance_between_leads: int | None = None,
        distance_from_lead: int | None = None,
    ) -> StudyLocusOverlap:
        """Find the overlapping signals in a particular set of associations (StudyLocus dataset).

        Args:
            method (StudyLocusOverlapMethod): Method to find the overlapping signals
            study_locus (StudyLocus): Study-locus associations to find the overlapping signals
            study_index (StudyIndex): Study index to find the overlapping signals
            distance_between_leads (int | None): Maximum distance between lead variants to consider two associations overlapping. Only necessary with the "distance" method.
            distance_from_lead (int | None): Maximum distance between a variant and the lead variant to consider it overlapping. Only necessary with the "distance" method.

        Returns:
            StudyLocusOverlap: Study-locus overlap dataset

        Raises:
            ValueError: If the method is not supported
        """
        if method.value not in [
            StudyLocusOverlapMethod.LD.value,
            StudyLocusOverlapMethod.DISTANCE.value,
        ]:
            raise ValueError(f"Unsupported method: {method}.")
        elif method.value == StudyLocusOverlapMethod.LD.value:
            return study_locus.find_overlaps_in_credible_set(study_index)
        elif (
            method.value == StudyLocusOverlapMethod.DISTANCE.value
            and distance_between_leads
            and distance_from_lead
        ):
            return study_locus.find_overlaps_in_locus(
                distance_between_leads, distance_from_lead
            )
        else:
            raise ValueError(
                f"Provide distance_between_leads and distance_from_lead to use the {method} method."
            )
