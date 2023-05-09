"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from otg.common.session import Session
from otg.config import FinnGenStepConfig
from otg.dataset.study_index import StudyIndexFinnGen


@dataclass
class FinnGenStep(FinnGenStepConfig):
    """FinnGen study table ingestion step."""

    session: Session = Session()

    def run(self: FinnGenStep) -> None:
        """Run FinnGen study table ingestion step."""
        # Read the JSON data from the URL.
        json_data = urlopen(self.finngen_phenotype_table_url).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([json_data])
        df = self.session.spark.read.json(rdd)

        # Parse the study index data.
        finngen_studies = StudyIndexFinnGen.from_source(
            df,
            self.finngen_release_prefix,
            self.finngen_sumstat_url_prefix,
            self.finngen_sumstat_url_suffix,
        )

        # Write the output.
        finngen_studies.df.write.mode(self.session.write_mode).parquet(
            self.finngen_study_index_out
        )
