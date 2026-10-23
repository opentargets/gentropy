"""Step to apply linkage based clumping on study-locus dataset."""

from __future__ import annotations

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.annotation import annotate_locus
from gentropy.method.clumping.locus_breaker import LocusBreakerClumping
from gentropy.method.clumping.window import WindowClumping


class LocusBreakerClumpingStep:
    """Step to perform locus-breaker clumping on a study."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        clumped_study_locus_output_path: str,
        lbc_baseline_pvalue: float,
        lbc_distance_cutoff: int,
        lbc_pvalue_threshold: float,
        lbc_flanking_distance: int,
        large_loci_size: int,
        wbc_clump_distance: int,
        wbc_pvalue_threshold: float,
        collect_locus: bool = False,
        remove_mhc: bool = True,
    ) -> None:
        """Run locus-breaker clumping step.

        This step will perform locus-breaker clumping on the full set of summary statistics.
        StudyLocus larger than the large_loci_size, by distance, will be further clumped with window-based
        clumping.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the input study locus.
            clumped_study_locus_output_path (str): path of the resulting, clumped study-locus dataset.
            lbc_baseline_pvalue (float): Baseline p-value for locus breaker clumping.
            lbc_distance_cutoff (int): Distance cutoff for locus breaker clumping.
            lbc_pvalue_threshold (float): P-value threshold for locus breaker clumping.
            lbc_flanking_distance (int): Flanking distance for locus breaker clumping.
            large_loci_size (int): Threshold distance to define large loci for window-based clumping.
            wbc_clump_distance (int): Clump distance for window breaker clumping.
            wbc_pvalue_threshold (float): P-value threshold for window breaker clumping.
            collect_locus (bool, optional): Whether to collect locus. Defaults to False.
            remove_mhc (bool, optional): If true will use exclude_region() to remove the MHC region.
        """
        sum_stats = SummaryStatistics.from_parquet(
            session,
            summary_statistics_input_path,
        )
        clumped_result = LocusBreakerClumping(
            baseline_significance=lbc_baseline_pvalue,
            gap=lbc_distance_cutoff,
            gwas_significance=lbc_pvalue_threshold,
            flanking_distance=lbc_flanking_distance,
            large_locus_size=large_loci_size,
            window=WindowClumping(
                distance=wbc_clump_distance, gwas_significance=wbc_pvalue_threshold
            ),
        ).clump(sum_stats)
        if remove_mhc:
            clumped_result = clumped_result.exclude_region(
                GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC),
                exclude_overlap=True,
            )

        if collect_locus:
            clumped_result = annotate_locus(clumped_result, sum_stats)
        clumped_result.df.write.partitionBy("id").mode(session.write_mode).parquet(
            clumped_study_locus_output_path
        )
