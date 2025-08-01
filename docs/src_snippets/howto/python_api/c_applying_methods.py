"""Docs to apply a method on a dataset."""
from __future__ import annotations

from gentropy import StudyLocus, SummaryStatistics


def apply_class_method_pics(study_locus_ld_annotated: StudyLocus) -> StudyLocus:
    """Docs to apply the PICS class method to mock study loci."""
    # --8<-- [start:apply_class_method_pics]
    from gentropy.method.pics import PICS

    finemapped_study_locus = PICS.finemap(
        study_locus_ld_annotated
    ).annotate_credible_sets()
    # --8<-- [end:apply_class_method_pics]
    return finemapped_study_locus


def apply_class_method_clumping(summary_stats: SummaryStatistics) -> StudyLocus:
    """Docs to apply the clumping class method to mock summary statistics."""
    # --8<-- [start:apply_class_method_clumping]
    from gentropy.method.window_based_clumping import WindowBasedClumping

    clumped_summary_statistics = WindowBasedClumping.clump(
        summary_stats, distance=250_000
    )
    # --8<-- [end:apply_class_method_clumping]
    return clumped_summary_statistics


def apply_instance_method(summary_stats: SummaryStatistics) -> StudyLocus:
    """Docs to apply the clumping instance method to mock summary statistics."""
    # --8<-- [start:apply_instance_method]
    # Perform window-based clumping on summary statistics
    # By default, the method uses a 1Mb window and a p-value threshold of 5e-8
    clumped_summary_statistics = summary_stats.window_based_clumping()
    # --8<-- [end:apply_instance_method]
    return clumped_summary_statistics
