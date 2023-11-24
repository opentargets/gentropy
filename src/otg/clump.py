"""Step to run clump summary statistics or study locus."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class ClumpStep:
    """Clumping step with customizable method.

    Attributes:
        session (Session): Session object.
        clump_method (ClumpMethod): Method of clumping to use.
        study_locus_in (str): Path to StudyLocus dataset to be clumped.
        study_index_path (str): Path to study index.
        ld_index_path (str): Path to LD index.
        clumped_study_locus_out (str): Output path for the clumped study locus dataset.
    """

    session: Session = MISSING
    summary_stats_path: str | None = None
    study_locus_path: str | None = None
    study_index_path: str = MISSING
    ld_index_path: str = MISSING
    clumped_study_locus_out: str = MISSING

    def __post_init__(self: ClumpStep) -> None:
        """Run the clumping step."""
        # Read
        ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)
        study_index = StudyIndex.from_parquet(self.session, self.study_index_path)
        if self.summary_stats_path:
            # Window-based clumping if summary statistics are provided
            study_locus_from_sumstats = SummaryStatistics.from_parquet(
                self.session, self.summary_stats_path
            ).window_based_clumping()

        if self.study_locus_path:
            # If an existing study locus is provided, read it in and combine it with the above
            study_locus = StudyLocus.from_parquet(self.session, self.study_locus_path)
            combined_study_locus = StudyLocus(
                _df=study_locus.df.unionByName(study_locus_from_sumstats.df),
                _schema=StudyLocus.get_schema(),
            )

        # Transform
        clumped_study_locus = combined_study_locus.annotate_ld(
            study_index=study_index, ld_index=ld_index
        ).clump()

        # Load
        clumped_study_locus.df.write.mode(self.session.write_mode).parquet(
            self.clumped_study_locus_out
        )
