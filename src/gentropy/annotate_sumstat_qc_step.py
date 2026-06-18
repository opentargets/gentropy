"""Step to annotate study index with pre-computed summary statistics QC metrics."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC


class AnnotateSumstatQCStep:
    """Step to annotate study index with pre-computed summary statistics QC metrics."""

    def __init__(
        self,
        session: Session,
        study_index_path: str,
        sumstats_qc_path: str,
        output_path: str,
        threshold_mean_beta: float = 0.05,
        threshold_mean_diff_pz: float = 0.05,
        threshold_se_diff_pz: float = 0.05,
        threshold_min_gc_lambda: float = 0.7,
        threshold_max_gc_lambda: float = 2.5,
        threshold_min_n_variants: int = 2_000_000,
    ) -> None:
        """Annotate study index with pre-computed summary statistics QC metrics.

        Reads a StudyIndex and a SummaryStatisticsQC dataset, joins the QC
        metrics onto the study index, applies configurable thresholds to
        populate qualityControls flags, and writes the annotated result.

        Args:
            session (Session): Spark session.
            study_index_path (str): Input path to the study index Parquet dataset.
            sumstats_qc_path (str): Input path to the pre-computed SummaryStatisticsQC Parquet dataset.
            output_path (str): Output path for the QC-annotated study index.
            threshold_mean_beta (float): Threshold for mean beta check. Defaults to 0.05.
            threshold_mean_diff_pz (float): Threshold for mean diff PZ check. Defaults to 0.05.
            threshold_se_diff_pz (float): Threshold for SE diff PZ check. Defaults to 0.05.
            threshold_min_gc_lambda (float): Minimum threshold for GC lambda check. Defaults to 0.7.
            threshold_max_gc_lambda (float): Maximum threshold for GC lambda check. Defaults to 2.5.
            threshold_min_n_variants (int): Minimum number of variants. Defaults to 2_000_000.
        """
        study_index = StudyIndex.from_parquet(session, study_index_path)
        sumstats_qc = SummaryStatisticsQC.from_parquet(session, sumstats_qc_path)

        (
            study_index.annotate_sumstats_qc(
                sumstats_qc,
                threshold_mean_beta=threshold_mean_beta,
                threshold_mean_diff_pz=threshold_mean_diff_pz,
                threshold_se_diff_pz=threshold_se_diff_pz,
                threshold_min_gc_lambda=threshold_min_gc_lambda,
                threshold_max_gc_lambda=threshold_max_gc_lambda,
                threshold_min_n_variants=threshold_min_n_variants,
            )
            .df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(output_path)
        )
