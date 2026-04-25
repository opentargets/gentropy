"""Step to apply linkage based clumping on study-locus dataset."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.locus_breaker_clumping import LocusBreakerClumping


class LocusBreakerClumpingStepConfig(BaseModel, frozen=True):
    """Config for LocusBreakerClumpingStep."""

    summary_statistics_input_path: Annotated[
        str, Field(description="Path to the input study locus.")
    ]
    clumped_study_locus_output_path: Annotated[
        str, Field(description="Path of the resulting, clumped study-locus dataset.")
    ]
    lbc_baseline_pvalue: Annotated[
        float, Field(description="Baseline p-value for locus breaker clumping.")
    ]
    lbc_distance_cutoff: Annotated[
        int, Field(description="Distance cutoff for locus breaker clumping.")
    ]
    lbc_pvalue_threshold: Annotated[
        float, Field(description="P-value threshold for locus breaker clumping.")
    ]
    lbc_flanking_distance: Annotated[
        int, Field(description="Flanking distance for locus breaker clumping.")
    ]
    large_loci_size: Annotated[
        int,
        Field(
            description="Threshold distance to define large loci for window-based clumping."
        ),
    ]
    wbc_clump_distance: Annotated[
        int, Field(description="Clump distance for window breaker clumping.")
    ]
    wbc_pvalue_threshold: Annotated[
        float, Field(description="P-value threshold for window breaker clumping.")
    ]
    collect_locus: Annotated[
        bool, Field(default=False, description="Whether to collect locus.")
    ]
    remove_mhc: Annotated[
        bool,
        Field(
            default=True,
            description="If true will use exclude_region() to remove the MHC region.",
        ),
    ]


class LocusBreakerClumpingStep:
    """Step to perform locus-breaker clumping on a study."""

    def __init__(
        self,
        session: Session,
        config: LocusBreakerClumpingStepConfig,
    ) -> None:
        """Run locus-breaker clumping step.

        This step will perform locus-breaker clumping on the full set of summary statistics.
        StudyLocus larger than the large_loci_size, by distance, will be further clumped with window-based
        clumping.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
        """
        sum_stats = SummaryStatistics.from_parquet(
            session,
            config.summary_statistics_input_path,
        )
        lbc = sum_stats.locus_breaker_clumping(
            config.lbc_baseline_pvalue,
            config.lbc_distance_cutoff,
            config.lbc_pvalue_threshold,
            config.lbc_flanking_distance,
        )
        wbc = sum_stats.window_based_clumping(
            config.wbc_clump_distance, config.wbc_pvalue_threshold
        )

        clumped_result = LocusBreakerClumping.process_locus_breaker_output(
            lbc,
            wbc,
            config.large_loci_size,
        )
        if config.remove_mhc:
            clumped_result = clumped_result.exclude_region(
                GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC),
                exclude_overlap=True,
            )

        if config.collect_locus:
            clumped_result = clumped_result.annotate_locus_statistics_boundaries(
                sum_stats
            )
        clumped_result.df.write.partitionBy("studyLocusId").mode(
            session.write_mode
        ).parquet(config.clumped_study_locus_output_path)
