"""Step to generate colocalisation results."""
from __future__ import annotations

from oxygen.common.session import Session
from oxygen.dataset.study_index import StudyIndex
from oxygen.dataset.study_locus import CredibleInterval, StudyLocus
from oxygen.method.colocalisation import ECaviar


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        study_index_path: str,
        coloc_path: str,
    ) -> None:
        """Run Colocalisation step.

        Args:
            session (Session): Session object.
            credible_set_path (str): Input credible sets path.
            study_index_path (str): Input study index path.
            coloc_path (str): Output Colocalisation path.
        """
        # Extract
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        si = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )

        # Transform
        overlaps = credible_set.filter_credible_set(
            CredibleInterval.IS95
        ).find_overlaps(si)
        ecaviar_results = ECaviar.colocalise(overlaps)

        # Load
        ecaviar_results.df.write.mode(session.write_mode).parquet(coloc_path)
