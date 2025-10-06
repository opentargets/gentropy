"""Step to run FinnGen UKBB MVP meta-analysis data ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import FinnGenMetaManifest
from gentropy.datasource.finngen_meta.study_index import FinnGenMetaStudyIndex
from gentropy.datasource.finngen_meta.summary_statistics import (
    FinnGenMetaSummaryStatistics,
)


class FinngenUkbbMvpMetaIngestionStep:
    """FinnGen UKBB MVP meta-analysis data ingestion and harmonisation."""

    def __init__(
        self,
        session: Session,
        source_manifest_path: str,
        source_summary_statistics_glob: str,
        efo_curation_path: str,
        gnomad_variant_index_path: str,
        study_index_output_path: str,
        raw_summary_statistics_output_path: str,
        harmonised_summary_statistics_output_path: str,
    ) -> None:
        """Data ingestion and harmonisation step for FinnGen UKB meta-analysis.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the manifest file.
            source_summary_statistics_glob (str): Glob pattern representing the location of summary statistics files.
            efo_curation_path (str): Path to the EFO curation file.
            gnomad_variant_index_path (str): Path to the gnomAD variant index file.
            study_index_output_path (str): Output path for the study index.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics.
        """
        session.logger.info(f"Reading Finngen manifest from {source_manifest_path}.")
        finngen_manifest = FinnGenMetaManifest.from_path(
            session=session, manifest_path=source_manifest_path
        )
        session.logger.info(f"Building study index for: {finngen_manifest.meta.value}")
        session.logger.info(f"Reading EFO curation from {efo_curation_path}.")
        efo_mapping = EFOMapping.from_path(
            session=session, efo_curation_path=efo_curation_path
        )

        session.logger.info("Creating study index.")
        study_index = FinnGenMetaStudyIndex.from_finngen_manifest(
            finngen_manifest, efo_mapping
        )
        session.logger.info("Writing study index.")
        study_index.df.write.mode(session.write_mode).parquet(study_index_output_path)
        session.logger.info(f"Study index written to {study_index_output_path}.")

        session.logger.info("Downloading summary statistics.")
        FinnGenMetaSummaryStatistics.bgzip_to_parquet(
            session=session,
            summary_statistics_glob=source_summary_statistics_glob,
            datasource=finngen_manifest.meta,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
        )
        session.logger.info("Preparing variant annotations.")
        # variant_annotations = VariantFlipper.from_variant_index(
        #     session=session, variant_index_path=gnomad_variant_index_path
        # )

        # session.logger.info("Harmonising summary statistics.")
        # harmonised_summary_statistics = FinngenUkbMetaMvpSummaryStats.from_source(
        #     session=session,
        #     raw_summary_statistics_path=raw_summary_statistics_output_path,
        #     variant_annotations=variant_annotations,
        #     output_path=harmonised_summary_statistics_output_path,
        # )
