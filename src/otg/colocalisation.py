"""Step to generate colocalisation results."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import CredibleInterval, StudyLocus
from otg.method.colocalisation import ECaviar


@dataclass
class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalization analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).

    Attributes:
        session (Session): Session object.
        credible_set_path (DictConfig): Input credible sets path.
        coloc_path (DictConfig): Output Colocalisation path.
    """

    session: Session = MISSING
    credible_set_path: str = MISSING
    study_index_path: str = MISSING
    coloc_path: str = MISSING

    def __post_init__(self: ColocalisationStep) -> None:
        """Run step."""
        # Extract
        credible_set = StudyLocus.from_parquet(
            self.session, self.credible_set_path, recursiveFileLookup=True
        )
        si = StudyIndex.from_parquet(
            self.session, self.study_index_path, recursiveFileLookup=True
        )

        # Transform
        overlaps = credible_set.filter_credible_set(
            CredibleInterval.IS95
        ).find_overlaps(si)
        ecaviar_results = ECaviar.colocalise(overlaps)

        # Load
        ecaviar_results.df.write.mode(self.session.write_mode).parquet(self.coloc_path)
