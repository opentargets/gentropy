"""deCODE ingestion step."""

from __future__ import annotations

from gentropy import (
    Session,
    StudyIndex,
    SummaryStatistics,
    SummaryStatisticsQC,
    TargetIndex,
)
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.decode import deCODEManifest
from gentropy.datasource.decode.study_index import deCODEStudyIndex
from gentropy.datasource.decode.summary_statistics import deCODESummaryStatistics
from gentropy.external.s3 import S3Config


class deCODEManifestGenerationStep:
    """Build deCODE Manifest from bucket listing.

    This step shall be run once to generate the listing of available deCODE datasets.
    The input to this step is the S3 bucket listing done via the aws s3 ls command.
    """

    def __init__(
        self,
        session: Session,
        s3_config_path: str,
        bucket_listing_path: str,
        output_path: str,
    ) -> None:
        """Run deCODE Manifest generation step."""
        config = S3Config.from_file(s3_config_path)

        manifest = deCODEManifest.from_bucket_listing(
            session=session,
            config=config,
            path=bucket_listing_path,
        )
        manifest.df.repartition(1).write.mode(session.write_mode).parquet(output_path)


class deCODEStudyIndexGenerationStep:
    """deCODE StudyIndex generation step."""

    def __init__(
        self,
        session: Session,
        manifest_path: str,
        target_index_path: str,
        output_path: str,
    ) -> None:
        """Run deCODE StudyIndex generation step."""
        manifest = deCODEManifest.from_path(session=session, path=manifest_path)
        target_index = TargetIndex.from_parquet(session=session, path=target_index_path)
        study_index = deCODEStudyIndex.from_manifest(
            manifest=manifest, target_index=target_index
        )

        study_index.df.repartition(1).write.mode(session.write_mode).parquet(
            output_path
        )


class deCODESummaryStatisticsIngestionStep:
    """deCODE SummaryStatistics ingestion step."""

    def __init__(
        self, session: Session, study_index_path: str, raw_summary_statistics_path: str
    ) -> None:
        """Run deCODE SummaryStatistics ingestion step."""
        study_index = StudyIndex.from_parquet(session, study_index_path)
        summary_statistics_paths = study_index.get_summary_statistics_paths()
        deCODESummaryStatistics.txtgz_to_parquet(
            session=session,
            summary_statistics_list=summary_statistics_paths,
            raw_summary_statistics_output_path=raw_summary_statistics_path,
            n_threads=deCODESummaryStatistics.N_THREAD_MAX,
        )


class deCODESummaryStatisticsHarmonisationStep:
    """deCODE SummaryStatistics harmonisation step."""

    def __init__(
        self,
        session: Session,
        raw_summary_statistics_path: str,
        gnomad_variant_direction_path: str,
        harmonised_summary_statistics_path: str,
        min_mac_threshold: int = 50,
        min_sample_size_threshold: int = 30_000,
        flipping_window_size: int = 10_000_000,
    ) -> None:
        """Run deCODE SummaryStatistics harmonisation step."""
        sumstat_harmonisation_config = {
            "min_mac_threshold": min_mac_threshold,
            "min_sample_size_threshold": min_sample_size_threshold,
            "flipping_window_size": flipping_window_size,
        }
        gvd = VariantDirection.from_parquet(session, gnomad_variant_direction_path)
        raw_summary_statistics = session.spark.read.parquet(raw_summary_statistics_path)
        harmonised_summary_statistics = deCODESummaryStatistics.from_source(
            raw_summary_statistics, gvd, **sumstat_harmonisation_config
        )
        harmonised_summary_statistics.df.write.mode(session.write_mode).partitionBy(
            "studyId"
        ).parquet(harmonised_summary_statistics_path)


class deCODESummaryStatisticsQCStep:
    """deCODE SummaryStatistics QC step."""

    def __init__(
        self,
        session: Session,
        harmonised_summary_statistics_path: str,
        harmonised_summary_statistics_qc_output_path: str,
        qc_summary_statistics_path: str,
        study_index_path: str,
        study_index_with_qc_output_path: str,
        qc_threshold: float = 1e-6,
    ) -> None:
        """Run deCODE SummaryStatistics QC step."""
        harmonised_summary_statistics = SummaryStatistics.from_parquet(
            session=session,
            path=harmonised_summary_statistics_path,
        )

        summary_statistics_qc = SummaryStatisticsQC.from_summary_statistics(
            gwas=harmonised_summary_statistics,
            pval_threshold=qc_threshold,
        )

        summary_statistics_qc.df.repartition(1).write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_qc_output_path
        )

        summary_statistics_qc.df.write.mode(session.write_mode).parquet(
            qc_summary_statistics_path
        )

        session.logger.info("Reading study index.")
        study_index = StudyIndex.from_parquet(session=session, path=study_index_path)
        session.logger.info("Annotating study index with QC information.")
        study_index = study_index.annotate_sumstats_qc(summary_statistics_qc)
        session.logger.info("Writing updated study index.")
        study_index.persist().df.repartition(1).write.mode(session.write_mode).parquet(
            study_index_with_qc_output_path
        )
