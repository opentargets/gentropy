"""Step to generate variant index dataset."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.variant_annotation import VariantAnnotation
from gentropy.dataset.variant_index import VariantIndex


class VariantIndexStep:
    """Run variant index step to only variants in study-locus sets.

    Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.
    """

    def __init__(
        self: VariantIndexStep,
        session: Session,
        variant_annotation_path: str,
        credible_set_path: str,
        variant_index_path: str,
    ) -> None:
        """Run VariantIndex step.

        Args:
            session (Session): Session object.
            variant_annotation_path (str): Variant annotation dataset path.
            credible_set_path (str): Credible set dataset path.
            variant_index_path (str): Variant index dataset path.
        """
        # Extract
        va = VariantAnnotation.from_parquet(session, variant_annotation_path)
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )

        # Transform
        vi = VariantIndex.from_variant_annotation(va, credible_set)

        (
            vi.df.write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(variant_index_path)
        )
