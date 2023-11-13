"""Step to run eQTL Catalogue study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from otg.datasource.eqtl_catalogue.summary_stats import EqtlCatalogueSummaryStats


@dataclass
class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step.

    Attributes:
        session (Session): Session object.
        eqtl_catalogue_paths_imported (str): eQTL Catalogue input files for the harmonised and imported data.
        eqtl_catalogue_study_index_out (str): Output path for the eQTL Catalogue study index dataset.
        eqtl_catalogue_summary_stats_out (str): Output path for the eQTL Catalogue summary stats.
    """

    session: Session = MISSING

    eqtl_catalogue_paths_imported: str = MISSING
    eqtl_catalogue_study_index_out: str = MISSING
    eqtl_catalogue_summary_stats_out: str = MISSING

    def __post_init__(self: EqtlCatalogueStep) -> None:
        """Run step."""
        # Fetch study index.
        df = self.session.spark.read.option("delimiter", "\t").csv(
            self.eqtl_catalogue_paths_imported, header=True
        )
        # Process partial study index.  At this point, it is not complete because we don't have the gene IDs, which we
        # will only get once the summary stats are ingested.
        study_index_df = EqtlCatalogueStudyIndex.from_source(df).df

        # Fetch summary stats.
        input_filenames = [row.summarystatsLocation for row in study_index_df.collect()]
        summary_stats_df = (
            self.session.spark.read.option("delimiter", "\t")
            .csv(input_filenames, header=True)
            .repartition(1280)
        )
        # Process summary stats.
        summary_stats_df = EqtlCatalogueSummaryStats.from_source(summary_stats_df).df

        # Add geneId column to the study index.
        study_index_df = EqtlCatalogueStudyIndex.add_gene_id_column(
            study_index_df,
            summary_stats_df,
        ).df

        # Write study index.
        study_index_df.write.mode(self.session.write_mode).parquet(
            self.eqtl_catalogue_study_index_out
        )
        # Write summary stats.
        (
            summary_stats_df.sortWithinPartitions("position")
            .write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.eqtl_catalogue_summary_stats_out)
        )
