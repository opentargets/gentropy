"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.dataset.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from otg.common.session import ETLSession


@dataclass
class VariantAnnotationStep:
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.
    """

    etl: ETLSession
    variant_annotation: DictConfig
    id: str = "variant_annotation"

    def run(self: VariantAnnotationStep) -> None:
        """Run variant annotation step."""
        self.etl.logger.info(f"Executing {self.id} step")
        variant_annotation = VariantAnnotation.from_gnomad(
            self.etl, **self.variant_annotation
        )

        # Writing data partitioned by chromosome and position:
        (
            variant_annotation.df.repartition(400, "chromosome")
            .sortWithinPartitions("chromosome", "position")
            .write.partitionBy("chromosome")
            .mode(self.etl.overwrite_mode)
            .parquet(self.variant_annotation.path)
        )
