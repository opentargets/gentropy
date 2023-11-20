"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.finngen.study_index import FinnGenStudyIndex
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats


@dataclass
class FinnGenStep:
    """FinnGen ingestion step.

    Attributes:
        session (Session): Session object.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        finngen_summary_stats_out (str): Output path for the FinnGen summary statistics.
    """

    session: Session = MISSING
    finngen_study_index_out: str = MISSING
    finngen_summary_stats_out: str = MISSING

    def __post_init__(self: FinnGenStep) -> None:
        """Run step."""
        # Fetch study index.
        # Process study index.
        study_index = FinnGenStudyIndex.from_source(self.session)
        # Write study index.
        study_index.df.write.mode(self.session.write_mode).parquet(
            self.finngen_study_index_out
        )

        # Fetch summary stats locations
        input_filenames = [row.summarystatsLocation for row in study_index.df.collect()]
        # Process summary stats.
        summary_stats = FinnGenSummaryStats.from_source(
            self.session, raw_files=input_filenames
        )

        # Write summary stats.
        (
            summary_stats.df.write.partitionBy("studyId")
            .mode(self.session.write_mode)
            .parquet(self.finngen_summary_stats_out)
        )
