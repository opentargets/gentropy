"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from otg.config import VariantAnnotationStepConfig
from otg.dataset.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from otg.common.session import Session


@dataclass
class VariantAnnotationStep(VariantAnnotationStepConfig):
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.
    """

    session: Session = SparkSession.builder.getOrCreate()

    def run(self: VariantAnnotationStep) -> None:
        """Run variant annotation step."""
        variant_annotation = VariantAnnotation.from_gnomad(
            self.etl,
            self.gnomad_genomes,
            self.chain_38_to_37,
            self.populations,
            self.variant_annotation_path,
        )
        # Writing data partitioned by chromosome and position:
        (
            variant_annotation.df.repartition(400, "chromosome")
            .sortWithinPartitions("chromosome", "position")
            .write.partitionBy("chromosome")
            .mode(self.etl.overwrite_mode)
            .parquet(self.variant_annotation_path)
        )
