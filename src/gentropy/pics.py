"""Step to apply PICS finemapping."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.method.pics import PICS


class PICSStepConfig(BaseModel, frozen=True):
    """Config for PICSStep."""

    study_locus_ld_annotated_in: Annotated[
        str, Field(description="Input LD annotated study-locus path.")
    ]
    picsed_study_locus_out: Annotated[
        str, Field(description="Output PICSed study-locus path.")
    ]


class PICSStep:
    """PICS finemapping of LD-annotated StudyLocus."""

    def __init__(
        self,
        session: Session,
        config: PICSStepConfig,
    ) -> None:
        """Run PICS on LD annotated study-locus.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
        """
        # Extract
        study_locus_ld_annotated = StudyLocus.from_parquet(
            session, config.study_locus_ld_annotated_in
        )
        # PICS
        (
            PICS.finemap(study_locus_ld_annotated)
            .filter_credible_set(credible_interval=CredibleInterval.IS99)
            # Flagging sub-significnat loci:
            .validate_lead_pvalue(
                pvalue_cutoff=WindowBasedClumpingStepConfig().gwas_significance
            )
            # Writing the output:
            .df.write.mode(session.write_mode)
            .parquet(config.picsed_study_locus_out)
        )
