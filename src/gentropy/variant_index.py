"""Step to generate variant index dataset."""

from __future__ import annotations

import math
from functools import reduce

from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.datasource.open_targets.variants import OpenTargetsVariant


class ConvertToVcfStep:
    """Convert dataset with variant annotation to VCF step.

    This step converts in-house data source formats to VCF like format.

    NOTE! Due to the csv DataSourceWriter limitations we can not save the column name
    `#CHROM` as in vcf file. The column is replaced with `CHROM`.
    """

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

        Raises:
            AssertionError: When the length of `source_paths` does not match the lenght of `source_formats`.
        """
        assert len(source_formats) == len(source_paths), (
            "Must provide format for each source path."
        )

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
        ).option("quoteAll", False).option("header", True).csv(output_path)
