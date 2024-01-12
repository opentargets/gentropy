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
        variant_id: Column,
        p_value_exponent: Column,
        p_value_mantissa: Column,
        ld_set: Column,
    ) -> Column:
        """Evaluates whether a lead variant is linked to a tag (with lowest p-value) in the same studyLocus dataset.

        Args:
            study_id (Column): studyId
            variant_id (Column): Lead variant id
            p_value_exponent (Column): p-value exponent
            p_value_mantissa (Column): p-value mantissa
            ld_set (Column): Array of variants in LD with the lead variant

        Returns:
            Column: Boolean in which True indicates that the lead is linked to another tag in the same dataset.
        """
        leads_in_study = f.collect_set(variant_id).over(Window.partitionBy(study_id))
        tags_in_studylocus = f.array_union(
            # Get all tag variants from the credible set per studyLocusId
            f.transform(ld_set, lambda x: x.tagVariantId),
            # And append the lead variant so that the intersection is the same for all studyLocusIds in a study
            f.array(variant_id),
        )
        intersect_lead_tags = f.array_sort(
            f.array_intersect(leads_in_study, tags_in_studylocus)
        )
        return (
            # If the lead is in the credible set, we rank the peaks by p-value
            f.when(
                f.size(intersect_lead_tags) > 0,
                f.row_number().over(
                    Window.partitionBy(study_id, intersect_lead_tags).orderBy(
                        p_value_exponent, p_value_mantissa
                    )
                )
                > 1,
            )
            # If the intersection is empty (lead is not in the credible set or cred set is empty), the association is not linked
            .otherwise(f.lit(False))
        )

    @classmethod
    def clump(cls: type[LDclumping], associations: StudyLocus) -> StudyLocus:
        """Perform clumping on studyLocus dataset.

        Args:
            associations (StudyLocus): StudyLocus dataset

        Returns:
            StudyLocus: including flag and removing locus information for LD clumped loci.
        """
        return associations.clump()
