"""Step to apply linkageg based clumping on study-locus dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import AnnotatedStudyLocus
from gentropy.method.annotation import annotate_ld
from gentropy.method.clumping.ld import LDClumping


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

        Leads are linked by LD at r2 0.5 or more in the study's major population
        (the `annotate_ld` default). Legacy annotated at r2 0.0, which also linked
        leads in LD only in another population of the LD index.

        Args:
            session (Session): Session object.
            study_locus_input_path (str): Path to the input study locus, annotated with its summary statistics.
            study_index_path (str): Path to the study index.
            ld_index_path (str): Path to the LD index.
            clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
        """
        study_locus = AnnotatedStudyLocus.from_parquet(session, study_locus_input_path)
        ld_index = LDIndex.from_parquet(session, ld_index_path)
        study_index = StudyIndex.from_parquet(session, study_index_path)

        # Annotating study locus with LD information, then flagging linked leads.
        # clump reads its input twice; persist so the LD index join runs once. The two
        # reads run in the same query, at the same time under adaptive execution, so the
        # cache is filled first: otherwise both reads compute the join.
        annotated = annotate_ld(study_locus, study_index, ld_index).persist()
        annotated.df.count()
        clumped = LDClumping(keep_clumped_loci=True).clump(annotated)
        clumped.df.write.mode(session.write_mode).parquet(
            clumped_study_locus_output_path
        )
