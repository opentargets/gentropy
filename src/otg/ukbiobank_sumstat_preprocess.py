"""Step to ingest UKBiobank summary statistics."""
from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import UKBiobankSumstatsPreprocessConfig
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class UKBiobankSumstatsPreprocessStep(UKBiobankSumstatsPreprocessConfig):
    """Step to ingest and process UKBiobank summary stats."""

    session: Session = Session()

    def run(self: UKBiobankSumstatsPreprocessStep) -> None:
        """Run Step."""
        # Extract
        self.session.logger.info(self.ukbiobank_sumstat_path)
        self.session.logger.info(self.ukbiobank_study_id)
        self.session.logger.info(self.ukbiobank_sumstats_out)

        # Reading summary statistics dataset:
        raw_dataset = self.session.spark.read.csv(
            self.ukbiobank_sumstat_path, header=True, sep="\t"
        )
        self.session.logger.info(
            f"Number of single point associations: {raw_dataset.count()}"
        )

        # Processing dataset:
        ukbb_summary_sumstats = SummaryStatistics.from_ukbiobank_summary_stats(
            raw_dataset, self.ukbiobank_study_id
        )
        ukbb_processed_sumstats = ukbb_summary_sumstats.calculate_confidence_interval()
        ukbb_processed_sumstats.df.write.mode(self.session.write_mode).parquet(
            self.ukbiobank_sumstats_out
        )
        self.session.logger.info("Processing dataset successfully completed.")
