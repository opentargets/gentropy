"""Step to ingest pre-computed FinnGen SuSIE finemapping results."""

from __future__ import annotations

import glob
from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.finngen.finngen_finemapping import FinnGenFinemapping


@dataclass
class FinnGenFinemappingIngestionStep(FinnGenFinemapping):
    """FinnGen study table ingestion step.

    Attributes:
        session (Session): Session object.
        finngen_finemapping_results_url (str): URL to the FinnGen SuSIE finemapping results.
        finngen_finemapping_summaries_url (str): FinnGen SuSIE summaries for CS filters(LBF>2).
        finngen_release_prefix (str): Release prefix for FinnGen.
        finngen_finemapping_out (str): Output path for the finemapping results in StudyLocus format.
    """

    session: Session = MISSING
    finngen_finemapping_results_url: str = MISSING
    finngen_finemapping_summaries_url: str = MISSING
    finngen_release_prefix: str = MISSING
    finngen_finemapping_out: str = MISSING

    def run(self: FinnGenFinemappingIngestionStep) -> None:
        """Run FinnGen finemapping ingestion step."""
        # Read finemapping outputs from the URL.

        input_filenames = glob.glob(self.finngen_finemapping_results_url)

        finngen_finemapping_df = self.session.spark.read.option("delimiter", "\t").csv(
            input_filenames, header=True
        )
        finngen_finemapping_summaries = self.session.spark.read.option(
            "delimiter", ","
        ).csv(self.finngen_finemapping_summaries_url, header=True)

        finngen_finemapping_df = FinnGenFinemapping.from_finngen_susie_finemapping(
            finngen_finemapping_df,
            finngen_finemapping_summaries,
            self.finngen_release_prefix,
        ).df

        # Write the output.
        finngen_finemapping_df.df.write.mode(self.session.write_mode).parquet(
            self.finngen_finemapping_out
        )
