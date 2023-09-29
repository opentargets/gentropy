"""Step to run UKBiobank study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import UKBiobankStepConfig
from otg.datasource.ukbiobank.study_index import UKBiobankStudyIndex


@dataclass
class UKBiobankStep(UKBiobankStepConfig):
    """UKBiobank study table ingestion step."""

    session: Session = Session()

    def run(self: UKBiobankStep) -> None:
        """Run UKBiobank study table ingestion step."""
        # Read in the UKBiobank manifest tsv file.
        df = self.session.spark.read.csv(
            self.ukbiobank_manifest, sep="\t", header=True, inferSchema=True
        )

        # Parse the study index data.
        ukbiobank_study_index = UKBiobankStudyIndex.from_source(df)

        # Write the output.
        ukbiobank_study_index.df.write.mode(self.session.write_mode).parquet(
            self.ukbiobank_study_index_out
        )
