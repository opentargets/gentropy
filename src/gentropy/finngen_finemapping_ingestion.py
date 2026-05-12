"""Step to ingest pre-computed FinnGen SuSIE finemapping results."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


class FinnGenFinemappingIngestionDefaults(BaseModel, frozen=True):
    """Defaults for FinnGenFinemappingIngestionStep.

    All values are frozen - create a new instance to override.
    """

    finngen_susie_finemapping_snp_files: Annotated[
        str,
        Field(
            default="gs://finngen-public-data-r11/finemap/full/susie/*.snp.bgz",
            description="Path to the FinnGen SuSIE finemapping results.",
        ),
    ]
    finngen_susie_finemapping_cs_summary_files: Annotated[
        str,
        Field(
            default="gs://finngen-public-data-r11/finemap/summary/*SUSIE.cred.summary.tsv",
            description="FinnGen SuSIE summaries for CS filters (LBF>2).",
        ),
    ]
    finngen_finemapping_lead_pvalue_threshold: Annotated[
        float,
        Field(
            default=1e-5,
            description="Lead p-value threshold.",
        ),
    ]
    finngen_release_prefix: Annotated[
        str,
        Field(
            default="FINNGEN_R11",
            description="FinnGen project release prefix. Should look like FINNGEN_R*.",
        ),
    ]


class FinnGenFinemappingIngestionStep:
    """FinnGen finemapping ingestion step."""

    def __init__(
        self,
        session: Session,
        config: FinnGenFinemappingIngestionDefaults,
        finngen_finemapping_out: str,
    ) -> None:
        """Run FinnGen finemapping ingestion step.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
            finngen_finemapping_out (str): Output path for the finemapping results in StudyLocus format.
        """
        # Read finemapping outputs from the input paths.
        finngen_release_prefix = FinnGenStudyIndex.validate_release_prefix(
            config.finngen_release_prefix
        )["prefix"]
        (
            FinnGenFinemapping.from_finngen_susie_finemapping(
                spark=session.spark,
                finngen_susie_finemapping_snp_files=config.finngen_susie_finemapping_snp_files,
                finngen_susie_finemapping_cs_summary_files=config.finngen_susie_finemapping_cs_summary_files,
                finngen_release_prefix=finngen_release_prefix,
            )
            # Flagging sub-significnat loci:
            .validate_lead_pvalue(
                pvalue_cutoff=config.finngen_finemapping_lead_pvalue_threshold
            )
            # Writing the output:
            .df.write.mode(session.write_mode)
            .parquet(finngen_finemapping_out)
        )
