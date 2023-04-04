"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl

from otg.common.session import Session
from otg.config import VariantAnnotationStepConfig
from otg.dataset.variant_annotation import VariantAnnotation


@dataclass
class VariantAnnotationStep(VariantAnnotationStepConfig):
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.
    """

    session: Session = Session()

    def run(self: VariantAnnotationStep) -> None:
        """Run variant annotation step."""
        # init hail session
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")

        """Run variant annotation step."""
        variant_annotation = VariantAnnotation.from_gnomad(
            self.gnomad_genomes,
            self.chain_38_to_37,
            self.populations,
        )
        # Writing data partitioned by chromosome and position:
        (
            variant_annotation.df.repartition(400, "chromosome")
            .sortWithinPartitions("chromosome", "position")
            .write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.variant_annotation_path)
        )
