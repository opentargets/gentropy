"""Step to apply PICS finemapping."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import LDAnnotatedStudyLocus
from gentropy.method.fine_mapping.pics import PICS


class PICSStep:
    """PICS finemapping of LD-annotated StudyLocus."""

    def __init__(
        self,
        session: Session,
        study_locus_ld_annotated_in: str,
        study_index_path: str,
        picsed_study_locus_out: str,
        picsed_credible_set_variant_out: str,
    ) -> None:
        """Run PICS on LD annotated study-locus.

        Loci whose lead is less significant than the GWAS significance threshold are not
        fine-mapped. Each fine-mapped locus gives one 95% credible set.

        Args:
            session (Session): Session object.
            study_locus_ld_annotated_in (str): Input LD annotated study-locus path.
            study_index_path (str): Study index path, for the study type of each credible set.
            picsed_study_locus_out (str): Output credible set path.
            picsed_credible_set_variant_out (str): Output credible set variant path.
        """
        # Extract
        loci = LDAnnotatedStudyLocus.from_parquet(
            session, study_locus_ld_annotated_in
        ).lead_pvalue_filter(WindowBasedClumpingStepConfig().gwas_significance)
        study_index = StudyIndex.from_parquet(session, study_index_path)
        # PICS
        credible_sets, credible_set_variants = PICS().finemap(loci, study_index)
        # Writing the outputs:
        credible_sets.df.write.mode(session.write_mode).parquet(picsed_study_locus_out)
        credible_set_variants.df.write.mode(session.write_mode).parquet(
            picsed_credible_set_variant_out
        )
