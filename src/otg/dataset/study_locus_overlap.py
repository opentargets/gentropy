"""Study locus overlap index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.dataset.study_index import StudyIndex
    from otg.dataset.study_locus import StudyLocus


@dataclass
class StudyLocusOverlap(Dataset):
    """Study-Locus overlap.

    This dataset captures pairs of overlapping `StudyLocus`: that is associations whose credible sets share at least one tagging variant.

    !!! note
        This is a helpful dataset for other downstream analyses, such as colocalisation. This dataset will contain the overlapping signals between studyLocus associations once they have been clumped and fine-mapped.
    """

    @classmethod
    def get_schema(cls: type[StudyLocusOverlap]) -> StructType:
        """Provides the schema for the StudyLocusOverlap dataset."""
        return parse_spark_schema("study_locus_overlap.json")

    @classmethod
    def from_associations(
        cls: type[StudyLocusOverlap], study_locus: StudyLocus, study_index: StudyIndex
    ) -> StudyLocusOverlap:
        """Find the overlapping signals in a particular set of associations (StudyLocus dataset).

        Args:
            study_locus (StudyLocus): Study-locus associations to find the overlapping signals
            study_index (StudyIndex): Study index to find the overlapping signals

        Returns:
            StudyLocusOverlap: Study-locus overlap dataset
        """
        return study_locus.find_overlaps(study_index)
