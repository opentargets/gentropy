# pylint: disable=too-many-arguments
"""Step to ingest pre-computed FinnGen SuSIE finemapping results."""

from __future__ import annotations

from dataclasses import dataclass

from gentropy.common.session import Session
from gentropy.config import FinngenFinemappingConfig
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


@dataclass
class FinnGenFinemappingIngestionStep(FinnGenFinemapping):
    """FinnGen study table ingestion step."""

    def __init__(
        self,
        session: Session,
        finngen_finemapping_out: str,
        finngen_susie_finemapping_snp_files: str = FinngenFinemappingConfig().finngen_susie_finemapping_snp_files,
        finngen_susie_finemapping_cs_summary_files: str = FinngenFinemappingConfig().finngen_susie_finemapping_cs_summary_files,
        finngen_finemapping_lead_pvalue_threshold: float = FinngenFinemappingConfig().finngen_finemapping_lead_pvalue_threshold,
        finngen_release_prefix: str = FinngenFinemappingConfig().finngen_release_prefix,
    ) -> None:
        """Run FinnGen finemapping ingestion step.

        Args:
            session (Session): Session object.
            finngen_finemapping_out (str): Output path for the finemapping results in StudyLocus format.
            finngen_susie_finemapping_snp_files(str): Path to the FinnGen SuSIE finemapping results.
            finngen_susie_finemapping_cs_summary_files (str): FinnGen SuSIE summaries for CS filters(LBF>2).
            finngen_finemapping_lead_pvalue_threshold (float): Lead p-value threshold.
            finngen_release_prefix (str): Finngen project release prefix. Should look like FINNGEN_R*.
        """
        # Read finemapping outputs from the input paths.
        finngen_release_prefix = FinnGenStudyIndex.validate_release_prefix(
            finngen_release_prefix
        )["prefix"]
        (
            FinnGenFinemapping.from_finngen_susie_finemapping(
                spark=session.spark,
                finngen_susie_finemapping_snp_files=finngen_susie_finemapping_snp_files,
                finngen_susie_finemapping_cs_summary_files=finngen_susie_finemapping_cs_summary_files,
                finngen_release_prefix=finngen_release_prefix,
            )
            # Flagging sub-significnat loci:
            .validate_lead_pvalue(
                pvalue_cutoff=finngen_finemapping_lead_pvalue_threshold
            )
            # Writing the output:
            .df.write.mode(session.write_mode)
            .parquet(finngen_finemapping_out)
        )
