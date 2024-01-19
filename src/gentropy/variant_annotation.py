"""Step to generate variant annotation dataset."""
from __future__ import annotations

import hail as hl

from gentropy.common.session import Session
from gentropy.datasource.gnomad.variants import GnomADVariants


class VariantAnnotationStep:
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.
    """

    def __init__(self, session: Session, variant_annotation_path: str) -> None:
        """Run Variant Annotation step.

        Args:
            session (Session): Session object.
            variant_annotation_path (str): Variant annotation dataset path.
        """
        # Initialise hail session.
        hl.init(sc=session.spark.sparkContext, log="/dev/null")
        # Run variant annotation.
        variant_annotation = GnomADVariants().as_variant_annotation()
        # Write data partitioned by chromosome and position.
        (
            variant_annotation.df.write.mode(session.write_mode).parquet(
                variant_annotation_path
            )
        )
