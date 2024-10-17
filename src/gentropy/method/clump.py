"""Clumps GWAS significant variants to generate a studyLocus dataset of independent variants."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.dataset.study_locus import StudyLocus


class LDclumping:
    """LD clumping reports the most significant genetic associations in a region in terms of a smaller number of “clumps” of genetically linked SNPs."""

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
            .orderBy(p_value_exponent.asc(), p_value_mantissa.asc())
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

    @classmethod
    def clump(cls: type[LDclumping], associations: StudyLocus) -> StudyLocus:
        """Perform clumping on studyLocus dataset.

        Args:
            associations (StudyLocus): StudyLocus dataset

        Returns:
            StudyLocus: including flag and removing locus information for LD clumped loci.
        """
        return associations.clump()
