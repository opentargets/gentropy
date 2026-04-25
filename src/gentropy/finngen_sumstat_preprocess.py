"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.datasource.finngen.summary_stats import FinnGenSummaryStats


class FinnGenSumstatPreprocessDefaults(BaseModel, frozen=True):
    """Defaults for FinnGenSumstatPreprocessStep.

    All values are frozen - create a new instance to override.
    """

    raw_sumstats_path: Annotated[
        str, Field(description="Input raw summary stats path.")
    ]
    out_sumstats_path: Annotated[str, Field(description="Output summary stats path.")]


class FinnGenSumstatPreprocessStep:
    """FinnGen sumstats preprocessing."""

    def __init__(
        self, session: Session, config: FinnGenSumstatPreprocessDefaults
    ) -> None:
        """Run FinnGen summary stats preprocessing step.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
        """
        # Process summary stats.
        (
            FinnGenSummaryStats.from_source(
                session.spark, raw_file=config.raw_sumstats_path
            )
            .df.write.mode(session.write_mode)
            .parquet(config.out_sumstats_path)
        )
