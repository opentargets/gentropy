"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import VariantIndexStepConfig
from otg.dataset.study_locus import StudyLocus
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex


@dataclass
class VariantIndexStep(VariantIndexStepConfig):
    """Variant index step.

    Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.
    """

    session: Session = Session()

    def run(self: VariantIndexStep) -> None:
        """Run variant index step."""
        # Variant annotation dataset
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)

        # Study-locus dataset
        study_locus = StudyLocus.from_parquet(self.session, self.study_locus_path)

        # Reduce scope of variant annotation dataset to only variants in study-locus sets:
        va_slimmed = va.filter_by_variant_df(
            study_locus.unique_lead_tag_variants(), ["id", "chromosome"]
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

        self.session.logger.info(f"Writing variant index to: {self.variant_index_path}")
        (
            vi.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.variant_index_path)
        )
