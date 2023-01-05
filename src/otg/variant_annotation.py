"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.data.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from otg.common.session import ETLSession


@dataclass
class VariantAnnotationStep:
    """Variant annotation step."""

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
