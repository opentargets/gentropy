"""Step to create study locus object from harmonised summary statistics parquets."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class SumstatsProcessStep:
    """Step to convert GWAS Catalog harmonised summary stats to study locus.

    Attributes:
        session (Session): Session object.
        sumstats_path (str): Harmonised summary statistics path.
        study_locus_path (str): Study locus after window-based clumping.
    """

    session: Session = MISSING
    sumstats_path: str = MISSING
    study_locus_path: str = MISSING

    def __post_init__(self: SumstatsProcessStep) -> None:
        """Run step."""
        # Extract
        sumstats = SummaryStatistics.from_parquet(self.session, self.sumstats_path)

        # Transform / Load
        sumstats.window_based_clumping().df.write.mode(self.session.write_mode).parquet(
            self.study_locus_path
        )
