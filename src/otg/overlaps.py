"""Step to generate the dataset of overlapping studyLocus associations."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap


@dataclass
class OverlapsIndexStep:
    """StudyLocus overlaps step.

    !!! note
    This dataset is defined to contain the overlapping signals between studyLocus associations once they have been clumped and fine-mapped.

    This step generates a dataset of overlapping studyLocus associations.

    Attributes:
        session (Session): Session object.
        study_locus_path (str): Input study-locus path.
        study_index_path (str): Input study index path to extract the type of study.
        overlaps_index_out (str): Output overlaps index path.
    """

    session: Session

    study_locus_path: str = MISSING
    study_index_path: str = MISSING
    overlaps_index_out: str = MISSING

    def __post_init__(self: OverlapsIndexStep) -> None:
        """Run step."""
        # Extract
        study_locus = StudyLocus.from_parquet(self.session, self.study_locus_path)
        study_index = StudyIndex.from_parquet(self.session, self.study_index_path)
        # Transform
        overlaps_index = StudyLocusOverlap.from_associations(study_locus, study_index)
        # Load
        overlaps_index.df.write.mode(self.session.write_mode).parquet(
            self.overlaps_index_out
        )
