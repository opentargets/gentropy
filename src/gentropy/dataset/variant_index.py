"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import nullify_empty_array
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.variant_annotation import VariantAnnotation


@dataclass
class VariantIndex(Dataset):
    """Variant index dataset.

    Variant index dataset is the result of intersecting the variant annotation dataset with the variants with V2D available information.
    """

    @classmethod
    def get_schema(cls: type[VariantIndex]) -> StructType:
        """Provides the schema for the VariantIndex dataset.

        Returns:
            StructType: Schema for the VariantIndex dataset
        """
        return parse_spark_schema("variant_index.json")

    @classmethod
    def from_variant_annotation(
        cls: type[VariantIndex],
        variant_annotation: VariantAnnotation,
        study_locus: StudyLocus,
    ) -> VariantIndex:
        """Initialise VariantIndex from pre-existing variant annotation dataset.

        Args:
            variant_annotation (VariantAnnotation): Variant annotation dataset
            study_locus (StudyLocus): Study locus dataset with the variants to intersect with the variant annotation dataset

        Returns:
            VariantIndex: Variant index dataset
        """
        unchanged_cols = [
            "variantId",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
            "chromosomeB37",
            "positionB37",
            "alleleType",
            "alleleFrequencies",
            "inSilicoPredictors",
        ]
        va_slimmed = variant_annotation.filter_by_variant_df(
            study_locus.unique_variants_in_locus()
        )
        return cls(
            _df=(
                va_slimmed.df.select(
                    *unchanged_cols,
                    f.col("vep.mostSevereConsequence").alias("mostSevereConsequence"),
                    # filters/rsid are arrays that can be empty, in this case we convert them to null
                    nullify_empty_array(f.col("rsIds")).alias("rsIds"),
                )
                .repartition(400, "chromosome")
                .sortWithinPartitions("chromosome", "position")
            ),
            _schema=cls.get_schema(),
        )
