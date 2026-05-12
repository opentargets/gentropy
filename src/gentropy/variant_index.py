"""Step to generate variant index dataset."""

from __future__ import annotations

import math
from functools import reduce
from typing import Annotated

from pydantic import BaseModel, Field
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.amino_acid_variants import AminoAcidVariants
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.ensembl.vep_parser import VariantEffectPredictorParser
from gentropy.datasource.open_targets.variants import OpenTargetsVariant


class VariantIndexDefaults(BaseModel, frozen=True):
    """Defaults for VariantIndexStep.

    All values are frozen - create a new instance to override.
    """

    vep_output_json_path: Annotated[
        str, Field(description="Variant effect predictor output path (in json format).")
    ]
    variant_index_path: Annotated[
        str, Field(description="Variant index dataset path to save resulting data.")
    ]
    hash_threshold: Annotated[
        int, Field(description="Hash threshold for variant identifier length.")
    ] = 300
    variant_annotations_path: Annotated[
        list[str] | None,
        Field(description="List of paths to extra variant annotation datasets."),
    ] = None
    amino_acid_change_annotations: Annotated[
        list[str] | None,
        Field(description="List of paths to amino-acid based variant annotations."),
    ] = None


class VariantIndexStep:
    """Generate variant index based on a VEP output in json format.

    The variant index is a dataset that contains variant annotations extracted from VEP output. It is expected that all variants in the VEP output are present in the variant index.
    There's an option to provide extra variant annotations to be added to the variant index eg. allele frequencies from GnomAD.
    """

    def __init__(
        self: VariantIndexStep,
        config: VariantIndexDefaults,
        session: Session,
    ) -> None:
        """Run VariantIndex step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        # Extract variant annotations from VEP output:
        variant_index = VariantEffectPredictorParser.extract_variant_index_from_vep(
            session.spark, config.vep_output_json_path, config.hash_threshold
        )

        # Process variant annotations if provided:
        if config.variant_annotations_path:
            for annotation_path in config.variant_annotations_path:
                # Read variant annotations from parquet:
                annotations = VariantIndex.from_parquet(
                    session=session,
                    path=annotation_path,
                    recursiveFileLookup=True,
                    id_threshold=config.hash_threshold,
                )

                # Update index with extra annotations:
                variant_index = variant_index.add_annotation(annotations)

        # If provided read amino-acid based annotation and enrich variant index:
        if config.amino_acid_change_annotations:
            for annotation_path in config.amino_acid_change_annotations:
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
            .parquet(config.variant_index_path)
        )


class ConvertToVcfDefaults(BaseModel, frozen=True):
    """Defaults for ConvertToVcfStep."""

    source_paths: Annotated[list[str], Field(description="Input dataset path.")]
    source_formats: Annotated[
        list[str], Field(description="Format of the input dataset.")
    ]
    output_path: Annotated[str, Field(description="Output VCF file path.")]
    partition_size: Annotated[
        int,
        Field(description="Approximate number of variants in each output partition."),
    ] = 2000


class ConvertToVcfStep:
    """Convert dataset with variant annotation to VCF step.

    This step converts in-house data source formats to VCF like format.

    NOTE! Due to the csv DataSourceWriter limitations we can not save the column name
    `#CHROM` as in vcf file. The column is replaced with `CHROM`.
    """

    def __init__(
        self,
        config: ConvertToVcfDefaults,
        session: Session,
    ) -> None:
        """Initialize step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        assert len(config.source_formats) == len(config.source_paths), (
            "Must provide format for each source path."
        )

        # Load
        raw_variants = [
            session.load_data(p, f)
            for p, f in zip(config.source_paths, config.source_formats, strict=True)
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
        n_partitions = int(math.ceil(variant_count / config.partition_size))
        partitioned_variants = (
            merged_variants.repartitionByRange(
                n_partitions, f.col("#CHROM"), f.col("POS")
            )
            .sortWithinPartitions(f.col("#CHROM").asc(), f.col("POS").asc())
            # Due to the large number of partitions ensure we do not lose the partitions before saving them
            .persist()
            # FIXME the #CHROM column is saved as "#CHROM" by pyspark which fails under VEP,
            # The native solution would be to implement the datasource with proper writer
            # see https://docs.databricks.com/en/pyspark/datasources.html.
            # Proposed solution will require adding # at the start of the first line of
            # vcf before processing it in orchestration.
            .withColumnRenamed("#CHROM", "CHROM")
        )
        # Write
        partitioned_variants.write.mode(session.write_mode).option("sep", "\t").option(
            "quote", ""
        ).option("quoteAll", False).option("header", True).csv(config.output_path)
