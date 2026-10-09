"""Clumps GWAS significant variants to generate a studyLocus dataset of independent variants."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Self

import pyspark.sql.functions as f
from pydantic import BaseModel, Field
from pyspark.sql import Window

if TYPE_CHECKING:
    from pyspark.sql import Column

from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityFlag
from gentropy.method.ld_annotation import LDAnnotate


class LDClumpingConfig(BaseModel):
    """LD clumping configuration."""

    r2_threshold: Annotated[float, Field(gt=0, lt=1)]
    """r^2 threshold to filter the LD variants."""


class LDClumping:
    """LD clumping reports the most significant genetic associations in a region in terms of a smaller number of “clumps” of genetically linked SNPs."""

    def __init__(self: Self, r2_threshold: float) -> None:
        """Initialize method with parameters."""
        self.config = LDClumpingConfig(r2_threshold=r2_threshold)

    def clump(
        self: Self,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        ld_index: LDIndex,
    ) -> StudyLocus:
        """Perform LD clumping of the studyLocus.

        Evaluates whether a lead variant is linked to a tag (with lowest p-value) in the same studyLocus dataset.

        Returns:
            CredibleSet: with empty credible sets for linked variants and QC flag.
        """
        ld = LDAnnotate(ld_index, self.config.r2_threshold)
        ld_annotated_study_locus = ld.annotate_study_locus(
            study_locus,
            study_index,
        ).persist()

        clumped_df = (
            ld_annotated_study_locus.df.withColumn(
                "isLeadLinked",
                self._is_lead_linked(
                    f.col("studyId"),
                    f.col("chromosome"),
                    f.col("variantId"),
                    f.col("pValueMantissa"),
                    f.col("pValueExponent"),
                    f.col("ldSet"),
                ),
            )
            .withColumn(
                "ldSet",
                f.when(f.col("isLeadLinked"), f.array()).otherwise(f.col("ldSet")),
            )
            .withColumn(
                "qualityControls",
                study_locus.update_quality_flag(
                    f.col("qualityControls"),
                    f.col("isLeadLinked"),
                    StudyLocusQualityFlag.LD_CLUMPED,
                ),
            )
            .drop("isLeadLinked")
            .withColumn(
                "ldSetPositions",
                f.transform(
                    f.col("ldSet"),
                    lambda i: f.split(i.getField("variantId"), "_").getItem(1),
                ),
            )
            .withColumn("locusStart", f.min("ldSetPositions"))
            .withColumn("locusEnd", f.max("ldSetPositions"))
            .withColumn(
                "studyLocusId", StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS)
            )
            .drop("ldSetPositions")
        )
        return StudyLocus(
            _df=clumped_df,
            _schema=StudyLocus.get_schema(),
        )

    @staticmethod
    def _is_lead_linked(
        study_id: Column,
        chromosome: Column,
        variant_id: Column,
        p_value_exponent: Column,
        p_value_mantissa: Column,
        ld_set: Column,
    ) -> Column:
        """Evaluates whether a lead variant is linked to a tag (with lowest p-value) in the same studyLocus dataset.

        Args:
            study_id (Column): studyId
            chromosome (Column): chromosome
            variant_id (Column): Lead variant id
            p_value_exponent (Column): p-value exponent
            p_value_mantissa (Column): p-value mantissa
            ld_set (Column): Array of variants in LD with the lead variant

        Returns:
            Column: Boolean in which True indicates that the lead is linked to another tag in the same dataset.
        """
        # Partitoning data by study and chromosome - this is the scope for looking for linked loci.
        # Within the partition, we order the data by increasing p-value, and we collect the more significant lead variants in the window.
        windowspec = (
            Window.partitionBy(study_id, chromosome)
            .orderBy(f.asc(p_value_exponent), f.asc(p_value_mantissa))
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        more_significant_leads = f.collect_set(variant_id).over(windowspec)

        # Collect all variants from the ld_set + adding the lead variant to the list to make sure that the lead is always in the list.
        tags_in_studylocus = f.array_distinct(
            f.array_union(
                f.array(variant_id),
                f.transform(ld_set, lambda x: x.getField("tagVariantId")),
            )
        )

        # If more than one tags of the ld_set can be found in the list of the more significant leads, the lead is linked.
        # Study loci without variantId is considered as not linked.
        # Also leads that were not found in the LD index is also considered as not linked.
        return f.when(
            variant_id.isNotNull(),
            f.size(f.array_intersect(more_significant_leads, tags_in_studylocus)) > 1,
        ).otherwise(f.lit(False))
