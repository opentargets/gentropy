"""Step to run FinnGen UKBB MVP meta-analysis data ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.dataset.variant_index import VariantIndex
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
        harmonised_summary_statistics_qc_output_path: str,
        qc_threshold: float = 1e-8,
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
            harmonised_summary_statistics_qc_output_path (str): Output path for harmonised summary statistics QC results.
            qc_threshold (float, optional): P-value threshold for QC. Defaults to 1e-8.
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

        session.logger.info("Reading gnomAD variant index.")
        gnomad_variant_index = VariantIndex.from_parquet(
            session, gnomad_variant_index_path
        )

        session.logger.info("Building variant direction annotations.")
        variant_direction = VariantDirection.from_variant_index(gnomad_variant_index)

        session.logger.info("Reading raw summary statistics.")
        raw_summary_statistics = session.spark.read.parquet(
            raw_summary_statistics_output_path
        )

        session.logger.info("Harmonising summary statistics.")
        harmonised_summary_statistics = FinnGenMetaSummaryStatistics.from_source(
            raw_summary_statistics=raw_summary_statistics,
            finngen_manifest=finngen_manifest,
            variant_annotations=variant_direction,
        )

        session.logger.info("Writing harmonised summary statistics.")
        harmonised_summary_statistics.df.write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_output_path
        )
        session.logger.info(
            f"Harmonised summary statistics written to {harmonised_summary_statistics_output_path}."
        )

        session.logger.info("Running summary statistics QC.")
        summary_statistics_qc = SummaryStatisticsQC.from_summary_statistics(
            gwas=harmonised_summary_statistics,
            pval_threshold=qc_threshold,
        )

        session.logger.info("Writing summary statistics QC results.")
        summary_statistics_qc.df.repartition(1).write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_qc_output_path
        )
        session.logger.info(
            f"Summary statistics QC results written to {harmonised_summary_statistics_qc_output_path}."
        )

        session.logger.info("Adding qc to the study index.")
        study_index = study_index.annotate_sumstats_qc(summary_statistics_qc)

        session.logger.info("Writing updated study index.")
        study_index.df.repartition(1).write.mode("overwrite").parquet(
            study_index_output_path
        )
        session.logger.info(
            f"Updated study index written to {study_index_output_path}."
        )
