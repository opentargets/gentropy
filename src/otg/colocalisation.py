"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import CredibleInterval, StudyLocus
from otg.method.colocalisation import Coloc, ECaviar


@dataclass
class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalization analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).

    Attributes:
        study_locus_path (DictConfig): Input Study-locus path.
        coloc_path (DictConfig): Output Colocalisation path.
        priorc1 (float): Prior on variant being causal for trait 1.
        priorc2 (float): Prior on variant being causal for trait 2.
        priorc12 (float): Prior on variant being causal for traits 1 and 2.
    """

    session: Session = Session()

    study_locus_path: str = MISSING
    study_index_path: str = MISSING
    coloc_path: str = MISSING
    priorc1: float = 1e-4
    priorc2: float = 1e-4
    priorc12: float = 1e-5

    def __post_init__(self: ColocalisationStep) -> None:
        """Run colocalisation step."""
        # Study-locus information
        sl = StudyLocus.from_parquet(self.session, self.study_locus_path)
        si = StudyIndex.from_parquet(self.session, self.study_index_path)

        # Study-locus overlaps for 95% credible sets
        sl_overlaps = sl.credible_set(CredibleInterval.IS95).overlaps(si)

        coloc_results = Coloc.colocalise(
            sl_overlaps, self.priorc1, self.priorc2, self.priorc12
        )
        ecaviar_results = ECaviar.colocalise(sl_overlaps)

        coloc_results.df.unionByName(ecaviar_results.df, allowMissingColumns=True)

        coloc_results.df.write.mode(self.session.write_mode).parquet(self.coloc_path)
