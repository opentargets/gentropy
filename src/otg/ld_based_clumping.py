"""Step to apply linkageg based clumping on study-locus dataset."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus


@dataclass
class LdBasedClumpingStep:
    """Step to perform LD-based clumping on study locus dataset.

    As a first step, study locus is enriched with population specific linked-variants.
    That's why the study index and the ld index is required for this step. Study loci are flaggged
    in the resulting dataset, which can be explained by a more significant association
    from the same study.

    Attributes:
        session (Session): Session object
        study_locus_input_path (str): Path to the input study locus.
        study_index_path (str): Path to the study index.
        ld_index_path (str): Path to the LD index.
        clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
    """

    session: Session = MISSING
    study_locus_input_path: str = MISSING
    study_index_path: str = MISSING
    ld_index_path: str = MISSING
    clumped_study_locus_output_path: str = MISSING

    def __post_init__(self: LdBasedClumpingStep) -> None:
        """Run clumping step."""
        study_locus = StudyLocus.from_parquet(self.session, self.study_locus_input_path)
        ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)
        study_index = StudyIndex.from_parquet(self.session, self.study_index_path)

        (
            study_locus
            # Annotating study locus with LD information:
            .annotate_ld(study_index, ld_index)
            .clump()
            # Save result:
            .df.write.mode(self.session.write_mode)
            .parquet(self.clumped_study_locus_output_path)
        )
