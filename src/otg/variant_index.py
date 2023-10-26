"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.study_locus import StudyLocus
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex


@dataclass
class VariantIndexStep:
    """Variant index step.

    Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.

    Attributes:
        variant_annotation_path (str): Input variant annotation path.
        study_locus_path (str): Input study-locus path.
        variant_index_path (str): Output variant index path.
    """

    session: Session = Session()

    variant_annotation_path: str = MISSING
    study_locus_path: str = MISSING
    variant_index_path: str = MISSING

    def __post_init__(self: VariantIndexStep) -> None:
        """Run variant index step to only variants in study-locus sets."""
        # Extract
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        study_locus = StudyLocus.from_parquet(
            self.session, self.study_locus_path, recursiveFileLookup=True
        )

        # Transform
        vi = VariantIndex.from_variant_annotation(va, study_locus)

        # Load
        self.session.logger.info(f"Writing variant index to: {self.variant_index_path}")
        (
            vi.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.variant_index_path)
        )
