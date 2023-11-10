"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl
from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.gnomad.variants import GnomADVariants


@dataclass
class VariantAnnotationStep:
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.

    Attributes:
        session (Session): Session object.
        variant_annotation_path (str): Output variant annotation path.
    """

    session: Session = MISSING
    variant_annotation_path: str = MISSING

    def __post_init__(self: VariantAnnotationStep) -> None:
        """Run step."""
        # Initialise hail session.
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")
        # Run variant annotation.
        variant_annotation = GnomADVariants().as_variant_annotation()
        # Write data partitioned by chromosome and position.
        (
            variant_annotation.df.repartition(400, "chromosome")
            .sortWithinPartitions("chromosome", "position")
            .write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.variant_annotation_path)
        )
