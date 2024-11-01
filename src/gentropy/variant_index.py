"""Step to generate variant index dataset."""

from __future__ import annotations

import math
from functools import reduce

from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.ensembl.vep_parser import VariantEffectPredictorParser
from gentropy.datasource.open_targets.variants import OpenTargetsVariant


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
        gnomad_variant_annotations_path: str | None = None,
    ) -> None:
        """Run VariantIndex step.

        Args:
            session (Session): Session object.
            vep_output_json_path (str): Variant effect predictor output path (in json format).
            variant_index_path (str): Variant index dataset path to save resulting data.
            hash_threshold (int): Hash threshold for variant identifier length.
            gnomad_variant_annotations_path (str | None): Path to extra variant annotation dataset.
        """
        # Extract variant annotations from VEP output:
        variant_index = VariantEffectPredictorParser.extract_variant_index_from_vep(
            session.spark, vep_output_json_path, hash_threshold
        )

        # Process variant annotations if provided:
        if gnomad_variant_annotations_path:
            # Read variant annotations from parquet:
            annotations = VariantIndex.from_parquet(
                session=session,
                path=gnomad_variant_annotations_path,
                recursiveFileLookup=True,
                id_threshold=hash_threshold,
            )

            # Update file with extra annotations:
            variant_index = variant_index.add_annotation(annotations)

        (
            variant_index.df.repartitionByRange("chromosome", "position")
            .sortWithinPartitions("chromosome", "position")
            .write.mode(session.write_mode)
            .parquet(variant_index_path)
        )


class ConvertToVcfStep:
    """Convert dataset with variant annotation to VCF step."""

    def __init__(
        self,
        session: Session,
        source_paths: list[str],
        source_formats: list[str],
        output_path: str,
        partition_size: int,
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            source_paths (list[str]): Input dataset path.
            source_formats (list[str]): Format of the input dataset.
            output_path (str): Output VCF file path.
            partition_size (int): Approximate number of variants in each output partition.
        """
        assert len(source_formats) == len(
            source_paths
        ), "Must provide format for each source path."

        # Load
        raw_variants = [
            session.load_data(p, f)
            for p, f in zip(source_paths, source_formats, strict=True)
        ]

        # Extract
        processed_variants = [
            OpenTargetsVariant.as_vcf_df(session, df) for df in raw_variants
        ]

        # Merge
        merged_variants = reduce(
            lambda x, y: x.unionByName(y), processed_variants
        ).drop_duplicates(["#CHROM", "POS", "REF", "ALT"])

        variant_count = merged_variants.count()
        n_partitions = int(math.ceil(variant_count / partition_size))
        partitioned_variants = (
            merged_variants.repartitionByRange(n_partitions, f.col("#CHROM"))
            .sortWithinPartitions(f.col("#CHROM").asc(), f.col("POS").asc())
            # Due to the large number of partitions ensure we do not lose the partitions before saving them
            .persist()
        )
        # Write
        partitioned_variants.write.csv(output_path, sep="\t", header=True)
