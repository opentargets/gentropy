"""Step to apply linkageg based clumping on study-locus dataset."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus


class LDBasedClumpingStep:
    """Step to perform LD-based clumping on study locus dataset.

    As a first step, study locus is enriched with population specific linked-variants.
    That's why the study index and the ld index is required for this step. Study loci are flaggged
    in the resulting dataset, which can be explained by a more significant association
    from the same study.
    """

    def __init__(
        self,
        session: Session,
        study_locus_input_path: str,
        study_index_path: str,
        ld_index_path: str,
        clumped_study_locus_output_path: str,
    ) -> None:
        """Run LD-based clumping step.

        Args:
            session (Session): Session object.
            study_locus_input_path (str): Path to the input study locus.
            study_index_path (str): Path to the study index.
            ld_index_path (str): Path to the LD index.
            clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
        """
        study_locus = StudyLocus.from_parquet(session, study_locus_input_path)
        ld_index = LDIndex.from_parquet(session, ld_index_path)
        study_index = StudyIndex.from_parquet(session, study_index_path)

        (
            study_locus
            # Annotating study locus with LD information:
            .annotate_ld(study_index, ld_index)
            .clump()
            # Save result:
            .df.write.mode(session.write_mode)
            .parquet(clumped_study_locus_output_path)
        )
