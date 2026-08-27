"""Step to apply linkage based clumping on study-locus dataset."""

from __future__ import annotations

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.locus_breaker_clumping import LocusBreakerClumping


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

        # Locus-breaker and window-based clumping each only ever consider rows
        # passing their own p-value cutoff, so they can share a single scan of the
        # summary statistics taken at the looser of the two thresholds. Both
        # methods re-apply their own cutoff internally, so this is a pure read
        # optimisation.
        #
        # The shared subset is written out and read back rather than held in an
        # in-memory cache. `persist()` on its own does not achieve a single scan:
        # nothing is materialised until the final write action, and at that point
        # three branches consume the subset -- locus-breaker once, window-based
        # twice, because process_locus_breaker_output uses it both as a semi-join
        # filter and to re-clump large loci. Those branches are independent, so
        # AQE submits them concurrently and each races to compute the same
        # partitions on a different executor. Measured on the GWAS Catalog run of
        # 2026-08-27: 993 of 1,004 sampled cache blocks were read, decompressed
        # and stored three times over.
        #
        # Checkpointing also fixes the partitioning. The filtered subset is ~0.02%
        # of the input, so scanning it in place leaves ~121k partitions holding
        # ~45KB each; every downstream window Exchange then has a 121k-partition
        # map side. Writing at a sane width shrinks the map side of all three by
        # two orders of magnitude.
        #
        # `sum_stats` deliberately stays unfiltered for
        # annotate_locus_statistics_boundaries below, which needs every variant
        # inside a locus window and not only the significant ones.
        significant_subset_path = (
            f"{clumped_study_locus_output_path}_significant_subset"
        )
        sum_stats.pvalue_filter(
            max(lbc_baseline_pvalue, wbc_pvalue_threshold)
        ).df.repartition(1000).write.mode("overwrite").parquet(significant_subset_path)
        significant = SummaryStatistics.from_parquet(session, significant_subset_path)

        lbc = significant.locus_breaker_clumping(
            lbc_baseline_pvalue,
            lbc_distance_cutoff,
            lbc_pvalue_threshold,
            lbc_flanking_distance,
        )
        wbc = significant.window_based_clumping(
            wbc_clump_distance, wbc_pvalue_threshold
        )

        clumped_result = LocusBreakerClumping.process_locus_breaker_output(
            lbc,
            wbc,
            large_loci_size,
        )
        if remove_mhc:
            clumped_result = clumped_result.exclude_region(
                GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC),
                exclude_overlap=True,
            )

        if collect_locus:
            clumped_result = clumped_result.annotate_locus_statistics_boundaries(
                sum_stats
            )
        clumped_result.df.write.partitionBy("studyLocusId").mode(
            session.write_mode
        ).parquet(clumped_study_locus_output_path)
