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
    """Run variant index step to only variants in study-locus sets.

    Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.

    Attributes:
        session (Session): Session object.
        variant_annotation_path (str): Input variant annotation path.
        study_locus_path (str): Input study-locus path.
        variant_index_path (str): Output variant index path.
    """

    session: Session = MISSING
    variant_annotation_path: str = MISSING
    credible_set_path: str = MISSING
    variant_index_path: str = MISSING

    def __post_init__(self: VariantIndexStep) -> None:
        """Run step."""
        # Extract
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        credible_set = StudyLocus.from_parquet(
            self.session, self.credible_set_path, recursiveFileLookup=True
        )

        # Transform
        vi = VariantIndex.from_variant_annotation(va, credible_set)

        (
            vi.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.variant_index_path)
        )
