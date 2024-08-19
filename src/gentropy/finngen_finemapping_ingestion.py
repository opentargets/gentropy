# pylint: disable=too-many-arguments
"""Step to ingest pre-computed FinnGen SuSIE finemapping results."""

from __future__ import annotations

from dataclasses import dataclass

from gentropy.common.session import Session
from gentropy.config import FinngenFinemappingConfig
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping


@dataclass
class FinnGenFinemappingIngestionStep(FinnGenFinemapping):
    """FinnGen study table ingestion step."""

    def __init__(
        self,
        session: Session,
        finngen_finemapping_out: str,
        finngen_susie_finemapping_snp_files: str = FinngenFinemappingConfig().finngen_susie_finemapping_snp_files,
        finngen_susie_finemapping_cs_summary_files: str = FinngenFinemappingConfig().finngen_susie_finemapping_cs_summary_files,
        finngen_release_prefix: str = FinngenFinemappingConfig().finngen_release_prefix,
    ) -> None:
        """Run FinnGen finemapping ingestion step.

        Args:
            session (Session): Session object.
            finngen_finemapping_out (str): Output path for the finemapping results in StudyLocus format.
            finngen_susie_finemapping_snp_files(str): Path to the FinnGen SuSIE finemapping results.
            finngen_susie_finemapping_cs_summary_files (str): FinnGen SuSIE summaries for CS filters(LBF>2).
            finngen_release_prefix (str): Release prefix for FinnGen.
        """
        # Read finemapping outputs from the input paths.

        finngen_finemapping_df = FinnGenFinemapping.from_finngen_susie_finemapping(
            spark=session,
            finngen_susie_finemapping_snp_files=finngen_susie_finemapping_snp_files,
            finngen_susie_finemapping_cs_summary_files=finngen_susie_finemapping_cs_summary_files,
            finngen_release_prefix=finngen_release_prefix,
        )

        # Write the output.
        finngen_finemapping_df.df.write.mode(session.write_mode).parquet(
            finngen_finemapping_out
        )
