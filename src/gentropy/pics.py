"""Step to apply PICS finemapping."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.method.pics import PICS


class PICSStep:
    """PICS finemapping of LD-annotated StudyLocus."""

    def __init__(
        self,
        session: Session,
        study_locus_ld_annotated_in: str,
        picsed_study_locus_out: str,
    ) -> None:
        """Run PICS on LD annotated study-locus.

        Args:
            session (Session): Session object.
            study_locus_ld_annotated_in (str): Input LD annotated study-locus path.
            picsed_study_locus_out (str): Output PICSed study-locus path.
        """
        # Extract
        study_locus_ld_annotated = StudyLocus.from_parquet(
            session, study_locus_ld_annotated_in
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
            .parquet(picsed_study_locus_out)
        )
