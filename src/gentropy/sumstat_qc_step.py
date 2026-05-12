"""Step to calculate quality control metrics on the provided GWAS study."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC


class SummaryStatisticsQCDefaults(BaseModel, frozen=True):
    """Defaults for SummaryStatisticsQCStep.

    All values are frozen - create a new instance to override.
    """

    gwas_path: Annotated[str, Field(description="Path to the GWAS summary statistics.")]
    output_path: Annotated[str, Field(description="Output path for the QC results.")]
    pval_threshold: Annotated[
        float, Field(description="P-value threshold for the QC.")
    ] = 5e-8


class SummaryStatisticsQCStep:
    """Step to run GWAS QC."""

    def __init__(
        self,
        config: SummaryStatisticsQCDefaults,
        session: Session,
    ) -> None:
        """Calculating quality control metrics on the provided GWAS study.

        Args:
            config: Step configuration defaults.
            session: Spark session
        """
        gwas = SummaryStatistics.from_parquet(session, path=config.gwas_path)

        (
            SummaryStatisticsQC.from_summary_statistics(
                gwas=gwas,
                pval_threshold=config.pval_threshold,
            )
            .df.repartition(1)
            .write.mode(session.write_mode)
            .parquet(config.output_path)
        )
