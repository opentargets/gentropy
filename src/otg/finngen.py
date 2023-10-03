"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from otg.common.session import Session
from otg.config import FinnGenStepConfig
from otg.datasource.finngen.study_index import FinnGenStudyIndex
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats


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
        finngen_studies = FinnGenStudyIndex.from_source(
            df,
            self.finngen_release_prefix,
            self.finngen_sumstat_url_prefix,
            self.finngen_sumstat_url_suffix,
        )

        # Write the study index output.
        finngen_studies.df.write.mode(self.session.write_mode).parquet(
            self.finngen_study_index_out
        )

        # Ingest summary statistics
        # This is a stub: final ingestion to be discussed further and implemented in a subsequent PR.
        for row in finngen_studies.collect():
            self.session.logger.info(
                f"Processing {row.studyId} with summary statistics in {row.summarystatsLocation}"
            )
            summary_stats_df = (
                self.session.spark.read.option("delimiter", "\t")
                .csv(row.summarystatsLocation, header=True)
                .repartition("#chrom")
            )

            # Process and output the data.
            out_filename = f"{self.finngen_summary_stats_out}/{row.studyId}"
            FinnGenSummaryStats.from_finngen_harmonized_summary_stats(
                summary_stats_df, row.finngen_study_id
            ).df.sortWithinPartitions("position").write.partitionBy("chromosome").mode(
                self.session.write_mode
            ).parquet(
                out_filename
            )
