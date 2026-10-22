"""Docs to apply a method on a dataset."""

from __future__ import annotations

from gentropy import StudyLocus, SummaryStatistics
from gentropy.dataset.credible_set import CredibleSet
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import LDAnnotatedStudyLocus


def apply_class_method_pics(
    study_locus_ld_annotated: LDAnnotatedStudyLocus, study_index: StudyIndex
) -> CredibleSet:
    """Docs to apply the PICS method to LD annotated study loci."""
    # --8<-- [start:apply_class_method_pics]
    from gentropy.method.fine_mapping.pics import PICS

    credible_sets, credible_set_variants = PICS().finemap(
        study_locus_ld_annotated, study_index
    )
    # --8<-- [end:apply_class_method_pics]
    return credible_sets


def apply_class_method_clumping(summary_stats: SummaryStatistics) -> StudyLocus:
    """Docs to apply the clumping class method to mock summary statistics."""
    # --8<-- [start:apply_class_method_clumping]
    from gentropy.method.clumping.window import WindowClumping

    clumped_summary_statistics = WindowClumping(
        distance=250_000, gwas_significance=5e-8
    ).clump(summary_stats)
    # --8<-- [end:apply_class_method_clumping]
    return clumped_summary_statistics


def apply_instance_method(summary_stats: SummaryStatistics) -> SummaryStatistics:
    """Docs to apply the p-value filter instance method to mock summary statistics."""
    # --8<-- [start:apply_instance_method]
    # Keep the associations at least as significant as 5e-8
    significant_summary_statistics = summary_stats.pvalue_filter(5e-8)
    # --8<-- [end:apply_instance_method]
    return significant_summary_statistics
