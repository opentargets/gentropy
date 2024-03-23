"""Step to apply PICS finemapping."""

from __future__ import annotations

from gentropy.common.session import Session
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
        picsed_sl = (
            PICS.finemap(study_locus_ld_annotated)
            .annotate_credible_sets()
            .filter_credible_set(credible_interval=CredibleInterval.IS99)
        )
        # Write
        picsed_sl.df.write.mode(session.write_mode).parquet(picsed_study_locus_out)
