"""Step to run clump summary statistics."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class ClumpStep:
    """Window-based clumping step, LD annotation and LD clumping and PICS of summary statistics.

    Attributes:
        session (Session): Session object.

        summary_stats_path (str): Path to summary statistics.
        study_index_path (str): Path to study index.
        ld_index_path (str): Path to LD index.
        clumped_study_locus_out (str): Output path for the clumped study locus dataset.
    """

    session: Session = MISSING
    summary_stats_path: str = MISSING
    study_index_path: str = MISSING
    ld_index_path: str = MISSING
    clumped_study_locus_out: str = MISSING

    def __post_init__(self: ClumpStep) -> None:
        """Run step."""
        # Extract
        summary_stats = SummaryStatistics.from_parquet(
            self.session, self.summary_stats_path
        )
        ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)
        study_index = StudyIndex.from_parquet(self.session, self.study_index_path)

        # Clumping
        study_locus = (
            summary_stats.window_based_clumping()
            .annotate_ld(study_index=study_index, ld_index=ld_index)
            .clump()
        )
        study_locus.df.write.mode(self.session.write_mode).parquet(
            self.clumped_study_locus_out
        )
