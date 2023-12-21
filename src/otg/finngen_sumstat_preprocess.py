"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats


@dataclass
class FinnGenSumstatPreprocessStep:
    """FinnGen sumstats preprocessing.

    Attributes:
        session (Session): Session object.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        finngen_summary_stats_out (str): Output path for the FinnGen summary statistics.
    """

    session: Session = MISSING
    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING

    def __post_init__(self: FinnGenSumstatPreprocessStep) -> None:
        """Run step."""
        # Process summary stats.
        (
            FinnGenSummaryStats.from_source(
                self.session.spark, raw_file=self.raw_sumstats_path
            )
            .df.write.mode(self.session.write_mode)
            .parquet(self.out_sumstats_path)
        )
