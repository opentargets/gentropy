"""Step to apply linkage based clumping on study-locus dataset."""

from __future__ import annotations

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.locus_breaker_clumping import LocusBreakerClumping


class LocusBreakerClumpingStep:
    """Identify lead variants from summary statistics using locus-breaker clumping.

    Algorithm
    ---------
    1. **LBC — variable-width locus identification**:

       a. Filter to `lbc_baseline_pvalue` (lenient threshold that defines which variants
          can form locus boundaries, e.g. 1×10⁻⁵).
       b. For each study/chromosome, compute the gap between each consecutive pair of
          significant variants sorted by position.
       c. Mark a new locus start wherever the gap exceeds `lbc_distance_cutoff` (or is at
          the first significant variant on a chromosome).
       d. Extend every locus boundary by `lbc_flanking_distance` on each end to capture
          flanking LD.
       e. Within each locus, select the most significant variant as the lead.
       f. Discard loci whose lead does not reach `lbc_pvalue_threshold` (strict threshold,
          e.g. 5×10⁻⁸).
       → Produces one lead per dynamically-sized locus with locusStart/locusEnd.

    2. **WBC — fixed-width fallback for large loci**:

       Run standard window-based clumping (see `WindowBasedClumpingStep`) on the same
       summary statistics with `wbc_clump_distance` and `wbc_pvalue_threshold`.

    3. **Merge** (`merge_lbc_with_wbc_for_large_loci`):

       - LBC loci with span ≤ `large_loci_size`: kept as-is.
       - LBC loci with span > `large_loci_size`: too wide for practical sumstat annotation;
         dropped and replaced by all WBC leads whose positions fall within those regions,
         each re-bounded to `position ± large_loci_size // 2`.

    4. **MHC removal** *(optional)*: exclude the MHC region when `remove_mhc=True`.

    5. **Annotate locus** *(optional)*: when `collect_locus=True`, join back to summary
       statistics and collect all variants within each lead's `[locusStart, locusEnd]`
       into a `locus[]` array using `annotate_locus_statistics_by_boundaries`.

    6. **Write** — partition by studyLocusId and write to Parquet. The output is a StudyLocus dataset.
    """

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
        collect_locus: bool,
        remove_mhc: bool,
    ) -> None:
        """Run locus-breaker clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the input summary statistics dataset.
            clumped_study_locus_output_path (str): Output path for the clumped study-locus dataset.
            lbc_baseline_pvalue (float): Lenient p-value threshold used to define which variants
                are considered when drawing locus boundaries (e.g. 1e-5).
            lbc_distance_cutoff (int): Minimum gap in base pairs between consecutive significant
                variants that separates two distinct loci.
            lbc_pvalue_threshold (float): Strict p-value threshold a locus lead must reach to be
                reported (e.g. 5e-8). Loci whose top hit does not pass this are discarded.
            lbc_flanking_distance (int): Base pairs added to each end of a locus after boundary
                detection, to capture flanking LD structure.
            large_loci_size (int): Span threshold in base pairs. LBC loci wider than this are
                replaced by WBC leads with fixed windows of this size (see step 3 above).
            wbc_clump_distance (int): Window half-width for the WBC fallback clumping.
            wbc_pvalue_threshold (float): P-value threshold for the WBC fallback clumping.
            collect_locus (bool): If True, annotate each lead with all sumstat variants in its
                [locusStart, locusEnd] window. If False, no locus annotation is performed.
            remove_mhc (bool): If True, exclude any study-locus overlapping the MHC region.
        """
        sum_stats = SummaryStatistics.from_parquet(
            session,
            summary_statistics_input_path,
        )
        lbc = sum_stats.locus_breaker_clumping(
            lbc_baseline_pvalue,
            lbc_distance_cutoff,
            lbc_pvalue_threshold,
            lbc_flanking_distance,
        )
        wbc = sum_stats.window_based_clumping(wbc_clump_distance, wbc_pvalue_threshold)

        clumped_result = LocusBreakerClumping.merge_lbc_with_wbc_for_large_loci(
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
            clumped_result = clumped_result.annotate_locus_statistics_by_boundaries(
                sum_stats
            )
        clumped_result.df.orderBy(
            "studyLocusId", "chromosome", "position"
        ).write.partitionBy("studyLocusId").mode(session.write_mode).parquet(
            clumped_study_locus_output_path
        )
