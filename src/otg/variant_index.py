"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from otg.common.session import ETLSession


@dataclass
class VariantIndexStep:
    """Variant index step."""

    etl: ETLSession
    variant_index: DictConfig
    variant_annotation: DictConfig
    id: str = "variant_index"

    def run(self: VariantIndex) -> None:
        """Step to generate variant index.

        Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.
        """
        self.etl.logger.info(f"Executing {self.id} step")

        vi = VariantIndex.from_credset(
            self.etl,
            self.variant_annotation.path,
            self.variant_index.credible_sets,
            self.variant_index.path,
        )

        # Write data:
        # self.etl.logger.info(
        #     f"Writing invalid variants from the credible set to: {self.variant_invalid}"
        # )
        # vi.invalid_variants.write.mode(self.etl.write_mode).parquet(
        #     self.variant_invalid
        # )

        self.etl.logger.info(f"Writing variant index to: {self.variant_index}")
        (
            vi.df.write.partitionBy("chromosome")
            .mode(self.etl.write_mode)
            .parquet(self.variant_index.path)
        )
