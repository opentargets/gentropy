"""Step to apply linkageg based clumping on study-locus dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus


class LocusBreakerClumpingStep:
    """Step to perform locus-breaker clumping on study locus dataset."""

    def __init__(
        self,
        session: Session,
        study_locus_input_path: str,
        study_index_path: str,
        clumped_study_locus_output_path: str,
    ) -> None:
        """Run locus-breaker clumping step.

        Args:
            session (Session): Session object.
            study_locus_input_path (str): Path to the input study locus.
            study_index_path (str): Path to the study index.
            clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
        """
        study_locus = StudyLocus.from_parquet(session, study_locus_input_path)

        (
            study_locus.clump()
            # Save result:
            .df.write.mode(session.write_mode)
            .parquet(clumped_study_locus_output_path)
        )
