"""Step to calculate quality control metrics on the provided GWAS study."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC


class SummaryStatisticsQCStep:
    """Step to run GWAS QC."""

    def __init__(
        self,
        session: Session,
        gwas_path: str,
        output_path: str,
        pval_threshold: float = 1e-8,
    ) -> None:
        """Calculating quality control metrics on the provided GWAS study.

        Args:
            session (Session): Spark session
            gwas_path (str): Path to the GWAS summary statistics.
            output_path (str): Output path for the QC results.
            pval_threshold (float): P-value threshold for the QC. Default is 1e-8.
        """
        gwas = SummaryStatistics.from_parquet(session, path=gwas_path)

        (
            SummaryStatisticsQC.from_summary_statistics(
                gwas=gwas,
                pval_threshold=pval_threshold,
            )
            .df.repartition(1)
            .write.mode(session.write_mode)
            .parquet(output_path)
        )
