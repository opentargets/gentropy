"""Step to generate the dataset of overlapping studyLocus associations."""
from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import StudyLocusOverlapStepConfig
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap


@dataclass
class OverlapsIndexStep(StudyLocusOverlapStepConfig):
    """StudyLocus overlaps step.

    This step generates a dataset of overlapping studyLocus associations.
    """

    session: Session = Session()

    def run(self: OverlapsIndexStep) -> None:
        """Run Overlaps index step.

        !!! note
            This dataset is defined to contain the overlapping signals between studyLocus associations once they have been clumped and fine-mapped.
        """
        # Extract
        study_locus = StudyLocus.from_parquet(self.session, self.study_locus_path)
        study_index = StudyIndex.from_parquet(self.session, self.study_index_path)
        # Transform
        overlaps_index = StudyLocusOverlap.from_associations(study_locus, study_index)
        # Load
        overlaps_index.df.write.mode(self.session.write_mode).parquet(
            self.overlaps_index_out
        )
