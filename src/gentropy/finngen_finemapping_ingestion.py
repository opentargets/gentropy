# pylint: disable=too-many-arguments
"""Step to ingest pre-computed FinnGen SuSIE finemapping results."""

from __future__ import annotations

from dataclasses import dataclass

from gentropy.common.session import Session
from gentropy.datasource.finngen.finngen_finemapping import FinnGenFinemapping


@dataclass
class FinnGenFinemappingIngestionStep(FinnGenFinemapping):
    """FinnGen study table ingestion step."""

    def __init__(
        self,
        session: Session,
        finngen_finemapping_results_url: str,
        finngen_finemapping_summaries_url: str,
        finngen_release_prefix: str,
        finngen_finemapping_out: str,
    ) -> None:
        """Run FinnGen finemapping ingestion step.

        Args:
            session (Session): Session object.
            finngen_finemapping_results_url (str): URL to the FinnGen SuSIE finemapping results.
            finngen_finemapping_summaries_url (str): FinnGen SuSIE summaries for CS filters(LBF>2).
            finngen_release_prefix (str): Release prefix for FinnGen.
            finngen_finemapping_out (str): Output path for the finemapping results in StudyLocus format.
        """
        # Read finemapping outputs from the URL.

        # finngen_finemapping_results = glob.glob(finngen_finemapping_results_url)
        # finngen_finemapping_summaries = glob.glob(finngen_finemapping_summaries_url)

        finngen_finemapping_df = FinnGenFinemapping.from_finngen_susie_finemapping(
            spark=session.spark,
            finngen_finemapping_df=finngen_finemapping_results_url,
            finngen_finemapping_summaries=finngen_finemapping_summaries_url,
            finngen_release_prefix=finngen_release_prefix,
        )

        # Write the output.
        finngen_finemapping_df.df.write.mode(session.write_mode).parquet(
            finngen_finemapping_out
        )
