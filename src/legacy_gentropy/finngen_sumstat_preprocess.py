"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.finngen.summary_stats import FinnGenSummaryStats


class FinnGenSumstatPreprocessStep:
    """FinnGen sumstats preprocessing."""

    def __init__(
        self, session: Session, raw_sumstats_path: str, out_sumstats_path: str
    ) -> None:
        """Run FinnGen summary stats preprocessing step.

        Args:
            session (Session): Session object.
            raw_sumstats_path (str): Input raw summary stats path.
            out_sumstats_path (str): Output summary stats path.
        """
        # Process summary stats.
        (
            FinnGenSummaryStats.from_source(session.spark, raw_file=raw_sumstats_path)
            .df.write.mode(session.write_mode)
            .parquet(out_sumstats_path)
        )
