"""Step to generate variant index dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.amino_acid_variants import AminoAcidVariants
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.ensembl.vep_parser import VariantEffectPredictorParser


class VariantIndexStep:
    """Generate variant index based on a VEP output in json format.

    The variant index is a dataset that contains variant annotations extracted from VEP output. It is expected that all variants in the VEP output are present in the variant index.
    There's an option to provide extra variant annotations to be added to the variant index eg. allele frequencies from GnomAD.
    """

    def __init__(
        self: VariantIndexStep,
        session: Session,
        vep_output_json_path: str,
        variant_index_path: str,
        hash_threshold: int,
        variant_annotations_path: list[str] | None = None,
        amino_acid_change_annotations: list[str] | None = None,
    ) -> None:
        """Run VariantIndex step.

        Args:
            session (Session): Session object.
            vep_output_json_path (str): Variant effect predictor output path (in json format).
            variant_index_path (str): Variant index dataset path to save resulting data.
            hash_threshold (int): Hash threshold for variant identifier length.
            variant_annotations_path (list[str] | None): List of paths to extra variant annotation datasets.
            amino_acid_change_annotations (list[str] | None): list of paths to amino-acid based variant annotations.
        """
        # Extract variant annotations from VEP output:
        variant_index = VariantEffectPredictorParser.extract_variant_index_from_vep(
            session.spark, vep_output_json_path, hash_threshold
        )

        # Process variant annotations if provided:
        if variant_annotations_path:
            for annotation_path in variant_annotations_path:
                # Read variant annotations from parquet:
                annotations = VariantIndex.from_parquet(
                    session=session,
                    path=annotation_path,
                    recursiveFileLookup=True,
                    id_threshold=hash_threshold,
                )

                # Update index with extra annotations:
                variant_index = variant_index.add_annotation(annotations)

        # If provided read amino-acid based annotation and enrich variant index:
        if amino_acid_change_annotations:
            for annotation_path in amino_acid_change_annotations:
                annotation_data = AminoAcidVariants.from_parquet(
                    session, annotation_path
                )

                # Update index with extra annotations:
                variant_index = variant_index.annotate_with_amino_acid_consequences(
                    annotation_data
                )

        (
            variant_index.df.repartitionByRange(
                session.output_partitions, "chromosome", "position"
            )
            .sortWithinPartitions("chromosome", "position")
            .write.mode(session.write_mode)
            .parquet(variant_index_path)
        )
