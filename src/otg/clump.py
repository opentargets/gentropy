"""Step to run clump summary statistics."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus


@dataclass
class ClumpStep:
    """LD annotation and LD clumping and PICS of a StudyLocus object.

    Attributes:
        session (Session): Session object.

        study_locus_in_path (str): Path to StudyLocus to be clumped.
        study_index_path (str): Path to study index to annotate ancestries.
        ld_index_path (str): Path to LD index.
        clumped_study_locus_out_path (str): Output path for the clumped study locus dataset.
    """

    session: Session = MISSING
    study_locus_in_path: str = MISSING
    study_index_path: str = MISSING
    ld_index_path: str = MISSING
    clumped_study_locus_out_path: str = MISSING

    def __post_init__(self: ClumpStep) -> None:
        """Run step."""
        # Extract
        study_locus = StudyLocus.from_parquet(self.session, self.study_locus_in_path)
        ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)
        study_index = StudyIndex.from_parquet(self.session, self.study_index_path)

        # Transform
        study_locus = study_locus.annotate_ld(
            study_index=study_index, ld_index=ld_index
        ).clump()

        # Load
        study_locus.df.write.mode(self.session.write_mode).parquet(
            self.clumped_study_locus_out_path
        )
