"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.finngen.study_index import FinnGenStudyIndex
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats


@dataclass
class FinnGenStep:
    """FinnGen ingestion step.

    Attributes:
        session (Session): Session object.
        finngen_phenotype_table_url (str): FinnGen API for fetching the list of studies.
        finngen_release_prefix (str): Release prefix pattern.
        finngen_sumstat_url_prefix (str): URL prefix for summary statistics location.
        finngen_sumstat_url_suffix (str): URL prefix suffix for summary statistics location.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        finngen_summary_stats_out (str): Output path for the FinnGen summary statistics.
    """

    session: Session = MISSING
    finngen_phenotype_table_url: str = MISSING
    finngen_release_prefix: str = MISSING
    finngen_sumstat_url_prefix: str = MISSING
    finngen_sumstat_url_suffix: str = MISSING
    finngen_study_index_out: str = MISSING
    finngen_summary_stats_out: str = MISSING

    def __post_init__(self: FinnGenStep) -> None:
        """Run step."""
        # Fetch study index.
        json_data = urlopen(self.finngen_phenotype_table_url).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([json_data])
        df = self.session.spark.read.json(rdd)
        # Process study index.
        study_index = FinnGenStudyIndex.from_source(
            df,
            self.finngen_release_prefix,
            self.finngen_sumstat_url_prefix,
            self.finngen_sumstat_url_suffix,
        )
        # Write study index.
        study_index.df.write.mode(self.session.write_mode).parquet(
            self.finngen_study_index_out
        )

        # Fetch summary stats.
        input_filenames = [row.summarystatsLocation for row in study_index.df.collect()]
        summary_stats_df = self.session.spark.read.option("delimiter", "\t").csv(
            input_filenames, header=True
        )
        # Process summary stats.
        summary_stats_df = FinnGenSummaryStats.from_source(summary_stats_df).df
        # Write summary stats.
        (
            summary_stats_df.sortWithinPartitions("position")
            .write.partitionBy("studyId", "chromosome")
            .mode(self.session.write_mode)
            .parquet(self.finngen_summary_stats_out)
        )
