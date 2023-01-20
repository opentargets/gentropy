"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.dataset.study_locus import StudyLocus
from otg.dataset.variant_annotation import VariantAnnotation
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

        # Variant annotation dataset
        va = VariantAnnotation.from_parquet(self.etl, self.variant_annotation.path)

        # Study-locus dataset
        study_locus = StudyLocus.from_parquet(self.etl, self.study_locus.path)

        # Reduce scope of variant annotation dataset to only variants in study-locus sets:
        va_slimmed = va.filter_by_variant_df(
            study_locus.unique_variants(), ["id", "chromosome"]
        )

        # Generate variant index ussing a subset of the variant annotation dataset
        vi = VariantIndex.from_variant_annotation(va_slimmed)

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
