"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.finngen.study_index import FinnGenStudyIndex


@dataclass
class FinnGenStudiesStep:
    """FinnGen study index generation step.

    Attributes:
        session (Session): Session object.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
    """

    session: Session = MISSING
    finngen_study_index_out: str = MISSING
    finngen_summary_stats_out: str = MISSING

    def __post_init__(self: FinnGenStudiesStep) -> None:
        """Run step."""
        # Fetch study index.
        FinnGenStudyIndex.from_source(self.session.spark).df.write.mode(
            self.session.write_mode
        ).parquet(self.finngen_study_index_out)
