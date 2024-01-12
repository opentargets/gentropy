"""Step to generate the dataset of overlapping studyLocus associations."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap


class OverlapsIndexStep:
    """StudyLocus overlaps step.

    !!! note
    This dataset is defined to contain the overlapping signals between studyLocus associations once they have been clumped and fine-mapped.

    This step generates a dataset of overlapping studyLocus associations.
    """

    def __init__(
        self,
        session: Session,
        study_locus_path: str,
        study_index_path: str,
        overlaps_index_out: str,
    ) -> None:
        """Run step to calculate overlaps between studyLocus associations.

        Args:
            session (Session): Session object.
            study_locus_path (str): Input studyLocus path.
            study_index_path (str): Input studyIndex path.
            overlaps_index_out (str): Output overlapsIndex path.
        """
        # Extract
        study_locus = StudyLocus.from_parquet(
            session, study_locus_path, recursiveFileLookup=True
        )
        study_index = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )
        # Transform
        overlaps_index = StudyLocusOverlap.from_associations(study_locus, study_index)
        # Load
        overlaps_index.df.write.mode(session.write_mode).parquet(overlaps_index_out)
