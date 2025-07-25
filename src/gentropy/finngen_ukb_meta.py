"""Step to run FinnGen UKB meta-analysis data ingestion."""

from __future__ import annotations

from gentropy.common.processing import (
    prepare_va,
    process_summary_stats_per_chromosome,
)
from gentropy.common.session import Session
from gentropy.datasource.finngen_ukb_meta.study_index import FinngenUkbMetaStudyIndex
from gentropy.datasource.finngen_ukb_meta.summary_stats import (
    FinngenUkbMetaSummaryStats,
)


class FinngenUkbMetaIngestionStep:
    """FinnGen UKB meta-analysis data ingestion and harmonisation."""

    def __init__(
        self, session: Session, raw_study_index_path_from_tsv: str, raw_summary_stats_path: str, variant_annotation_path: str, tmp_variant_annotation_path: str, study_index_output_path: str, summary_stats_output_path: str
    ) -> None:
        """Data ingestion and harmonisation step for FinnGen UKB meta-analysis.

        Args:
            session (Session): Session object.
            raw_study_index_path_from_tsv (str): Input raw study index path.
            raw_summary_stats_path (str): Input raw summary stats path.
            variant_annotation_path (str): Input variant annotation dataset path.
            tmp_variant_annotation_path (str): Temporary output path for variant annotation dataset.
            study_index_output_path (str): Study index output path.
            summary_stats_output_path (str): Summary stats output path.
        """
        session.logger.info("Pre-compute the direct and flipped variant annotation dataset.")
        prepare_va(session, variant_annotation_path, tmp_variant_annotation_path)

        session.logger.info("Process study index.")
        (
            FinngenUkbMetaStudyIndex.from_source(
                spark=session.spark,
                raw_study_index_path_from_tsv=raw_study_index_path_from_tsv,
            )
            .df
            .write
            .mode(session.write_mode)
            .parquet(study_index_output_path)
        )

        session.logger.info("Process and harmonise summary stats.")
        process_summary_stats_per_chromosome(session, FinngenUkbMetaSummaryStats, raw_summary_stats_path, tmp_variant_annotation_path, summary_stats_output_path, study_index_output_path)
