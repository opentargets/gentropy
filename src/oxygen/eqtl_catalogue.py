"""Step to run eQTL Catalogue study table ingestion."""

from __future__ import annotations

from oxygen.common.session import Session
from oxygen.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from oxygen.datasource.eqtl_catalogue.summary_stats import EqtlCatalogueSummaryStats


class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step."""

    def __init__(
        self,
        session: Session,
        eqtl_catalogue_paths_imported: str,
        eqtl_catalogue_study_index_out: str,
        eqtl_catalogue_summary_stats_out: str,
    ) -> None:
        """Run eQTL Catalogue ingestion step.

        Args:
            session (Session): Session object.
            eqtl_catalogue_paths_imported (str): Input eQTL Catalogue study index path.
            eqtl_catalogue_study_index_out (str): Output eQTL Catalogue study index path.
            eqtl_catalogue_summary_stats_out (str): Output eQTL Catalogue summary stats path.
        """
        # Fetch study index.
        df = session.spark.read.option("delimiter", "\t").csv(
            eqtl_catalogue_paths_imported, header=True
        )
        # Process partial study index.  At this point, it is not complete because we don't have the gene IDs, which we
        # will only get once the summary stats are ingested.
        study_index_df = EqtlCatalogueStudyIndex.from_source(df).df

        # Fetch summary stats.
        input_filenames = [row.summarystatsLocation for row in study_index_df.collect()]
        summary_stats_df = (
            session.spark.read.option("delimiter", "\t")
            .csv(input_filenames, header=True)
            .repartition(1280)
        )
        # Process summary stats.
        summary_stats_df = EqtlCatalogueSummaryStats.from_source(summary_stats_df).df

        # Add geneId column to the study index.
        study_index_df = EqtlCatalogueStudyIndex.add_gene_to_study_id(
            study_index_df,
            summary_stats_df,
        ).df

        # Write study index.
        study_index_df.write.mode(session.write_mode).parquet(
            eqtl_catalogue_study_index_out
        )
        # Write summary stats.
        (
            summary_stats_df.sortWithinPartitions("position")
            .write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(eqtl_catalogue_summary_stats_out)
        )
