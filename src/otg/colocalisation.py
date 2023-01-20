"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.dataset.study_locus import CredibleInterval, StudyLocus
from otg.method.colocalisation import Coloc, ECaviar

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from otg.common.session import ETLSession


@dataclass
class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalization analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    etl: ETLSession
    study_locus: DictConfig
    coloc: DictConfig
    id: str = "colocalisation"

    def run(self: ColocalisationStep) -> None:
        """Run colocalisation step."""
        self.etl.logger.info(f"Executing {self.id} step")

        # Study-locus information
        sl = StudyLocus.from_parquet(self.etl, self.study_locus.path)

        # Study-locus overlaps for 95% credible sets
        sl_overlaps = sl.credible_set(CredibleInterval.IS95).overlaps()

        coloc_results = Coloc.colocalise(sl_overlaps)
        ecaviar_results = ECaviar.colocalise(sl_overlaps)

        coloc_results.unionByName(ecaviar_results, allowMissingColumns=True)

        coloc_results.write.mode(self.etl.write_mode).parquet(self.coloc.path)
