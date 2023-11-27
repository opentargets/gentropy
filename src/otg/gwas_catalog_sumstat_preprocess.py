"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.gwas_catalog.summary_statistics import GWASCatalogSummaryStatistics


@dataclass
class GWASCatalogSumstatsPreprocessStep:
    """Step to preprocess GWAS Catalog harmonised summary stats.

    Attributes:
        session (Session): Session object.
        raw_sumstats_path (str): Input raw GWAS Catalog summary statistics path.
        out_sumstats_path (str): Output GWAS Catalog summary statistics path.
    """

    session: Session = MISSING
    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING

    def __post_init__(self: GWASCatalogSumstatsPreprocessStep) -> None:
        """Run step."""
        # Extract
        self.session.logger.info(self.raw_sumstats_path)
        self.session.logger.info(self.out_sumstats_path)

        self.session.logger.info(
            f"Ingesting summary stats from: {self.raw_sumstats_path}"
        )

        # Processing dataset:
        GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            self.session.spark, self.raw_sumstats_path
        ).df.write.mode(self.session.write_mode).parquet(self.out_sumstats_path)
        self.session.logger.info("Processing dataset successfully completed.")
