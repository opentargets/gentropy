"""Step to run FinnGen UKBB MVP meta-analysis data ingestion."""

from __future__ import annotations

from gentropy.common.session import Session


class FinngenUkbbMvpMetaIngestionStep:
    """FinnGen UKBB MVP meta-analysis data ingestion and harmonisation."""

    def __init__(
        self,
        session: Session,
        manifest_path: str,
        efo_curation_path: str,
        summary_statistics_base_path: str,
        gnomad_variant_index_path: str,
        study_index_output_path: str,
        raw_summary_statistics_output_path: str,
        harmonised_summary_statistics_output_path: str,
    ) -> None:
        """Data ingestion and harmonisation step for FinnGen UKB meta-analysis.

        Args:
            session (Session): Session object.
            manifest_path (str): Path to the manifest file.
            efo_curation_path (str): Path to the EFO curation file.
            summary_statistics_base_path (str): Base path where summary statistics files are located.
            gnomad_variant_index_path (str): Path to the gnomAD variant index file.
            study_index_output_path (str): Output path for the study index.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics.
        """
        session.logger.info(f"Reading Finngen manifest from {manifest_path}.")
        finngen_manifest = FinngenMetaManifest.from_path(
            session=session, manifest_path=manifest_path
        )

        session.logger.info(f"Reading EFO curation from {efo_curation_path}.")

        efo_curation = (
            session.spark.read.option("header", True)
            .option("sep", "\t")
            .csv(efo_curation_path)
        )

        study_index = FinngenMetaStudyIndex.from_manifest(
            session=session,
            manifest_path=manifest_path,
            output_path=study_index_output_path,
        )

        session.logger.info("Downloading summary statistics.")
        raw_summary_stats = FinngenUkbMvpMetaSummaryStats.gzip_to_parquet(
            session=session,
            study_index=study_index,
            summary_statistics_base_path=summary_statistics_base_path,
            output_path=raw_summary_statistics_output_path,
        )
        session.logger.info("Preparing variant annotations.")
        variant_annotations = VariantFlipper.from_variant_index(
            session=session, variant_index_path=gnomad_variant_index_path
        )

        session.logger.info("Harmonising summary statistics.")
        harmonised_summary_statistics = FinngenUkbMetaMvpSummaryStats.from_source(
            session=session,
            raw_summary_statistics_path=raw_summary_statistics_output_path,
            variant_annotations=variant_annotations,
            output_path=harmonised_summary_statistics_output_path,
        )
