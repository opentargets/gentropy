"""Step to run eQTL Catalogue study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.eqtl_catalogue.study_index import EqtlStudyIndex
from otg.datasource.eqtl_catalogue.summary_stats import EqtlSummaryStats


@dataclass
class EqtlStep:
    """eQTL Catalogue ingestion step.

    Attributes:
        session (Session): Session object.
        eqtl_catalogue_paths_imported (str): eQTL Catalogue input files for the harmonised and imported data.
        eqtl_catalogue_study_index_out (str): Output path for the eQTL Catalogue study index dataset.
        eqtl_catalogue_summary_stats_out (str): Output path for the eQTL Catalogue summary stats.
    """

    session: Session = Session()

    eqtl_catalogue_paths_imported: str = MISSING
    eqtl_catalogue_study_index_out: str = MISSING
    eqtl_catalogue_summary_stats_out: str = MISSING

    def __post_init__(self: EqtlStep) -> None:
        """Run step."""
        # Fetch study index.
        tsv_data = urlopen(self.eqtl_catalogue_paths_imported).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([tsv_data])
        df = self.session.spark.read.option("delimiter", "\t").csv(rdd)
        # Process study index.
        study_index = EqtlStudyIndex.from_source(df)
        # Write study index.
        study_index.df.write.mode(self.session.write_mode).parquet(
            self.eqtl_catalogue_study_index_out
        )

        # Fetch summary stats.
        input_filenames = [row.summarystatsLocation for row in study_index.collect()]
        summary_stats_df = self.session.spark.read.option("delimiter", "\t").csv(
            input_filenames, header=True
        )
        # Process summary stats.
        summary_stats_df = EqtlSummaryStats.from_source(summary_stats_df).df
        # Write summary stats.
        (
            summary_stats_df.sortWithinPartitions("position")
            .write.partitionBy("studyId", "chromosome")
            .mode(self.session.write_mode)
            .parquet(self.eqtl_catalogue_summary_stats_out)
        )
