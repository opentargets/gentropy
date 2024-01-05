"""Step to generate variant annotation dataset."""
from __future__ import annotations

from otg.common.session import Session
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import CredibleInterval, StudyLocus
from otg.method.colocalisation import Coloc, ECaviar


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalization analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    def __init__(
        self,
        session: Session,
        study_locus_path: str,
        study_index_path: str,
        coloc_path: str,
        priorc1: float = 1e-4,
        priorc2: float = 1e-4,
        priorc12: float = 1e-5,
    ) -> None:
        """Run Colocalisation step.

        Args:
            session (Session): Session object.
            study_locus_path (str): Input Study-locus path.
            study_index_path (str): Input Study-index path.
            coloc_path (str): Output Colocalisation path.
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4.
            priorc12 (float): Prior on variant being causal for traits 1 and 2. Defaults to 1e-5.
        """
        # Study-locus information
        sl = StudyLocus.from_parquet(session, study_locus_path)
        si = StudyIndex.from_parquet(session, study_index_path)

        # Study-locus overlaps for 95% credible sets
        sl_overlaps = sl.filter_credible_set(CredibleInterval.IS95).find_overlaps(si)

        coloc_results = Coloc.colocalise(sl_overlaps, priorc1, priorc2, priorc12)
        ecaviar_results = ECaviar.colocalise(sl_overlaps)

        coloc_results.df.unionByName(ecaviar_results.df, allowMissingColumns=True)

        coloc_results.df.write.mode(session.write_mode).parquet(coloc_path)
