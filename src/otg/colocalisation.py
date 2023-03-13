"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from otg.config import ColocalisationStepConfig
from otg.dataset.study_locus import CredibleInterval, StudyLocus
from otg.method.colocalisation import Coloc, ECaviar

if TYPE_CHECKING:
    from otg.common.session import Session


@dataclass
class ColocalisationStep(ColocalisationStepConfig):
    """Colocalisation step.

    This workflow runs colocalization analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    session: Session = SparkSession.builder.getOrCreate()

    def run(self: ColocalisationStep) -> None:
        """Run colocalisation step."""
        # Study-locus information
        sl = StudyLocus.from_parquet(self.session, self.study_locus_path)

        # Study-locus overlaps for 95% credible sets
        sl_overlaps = sl.credible_set(CredibleInterval.IS95).overlaps()

        coloc_results = Coloc.colocalise(
            sl_overlaps, self.priorc1, self.priorc2, self.priorc12
        )
        ecaviar_results = ECaviar.colocalise(sl_overlaps)

        coloc_results.unionByName(ecaviar_results, allowMissingColumns=True)

        coloc_results.write.mode(self.session.write_mode).parquet(self.coloc_path)
