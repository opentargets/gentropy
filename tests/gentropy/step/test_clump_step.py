"""Test clump step."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from gentropy.dataset.study_locus import AnnotatedStudyLocus
from gentropy.locus_breaker_clumping import LocusBreakerClumpingStep
from gentropy.window_based_clumping import WindowBasedClumpingStep

if TYPE_CHECKING:
    from gentropy.common.session import Session


class TestClumpStep:
    """Test clump step."""

    def test_clumpstep_summary_stats(self, session: Session) -> None:
        """Test clump step on summary statistics writes results to a temporary directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            clumped_study_locus_path = Path(temp_dir, "GCST005523_chr18_clumped")
            WindowBasedClumpingStep(
                session=session,
                summary_statistics_input_path="tests/gentropy/data_samples/sumstats_sample",
                study_locus_output_path=str(clumped_study_locus_path),
            )
            assert Path(clumped_study_locus_path).exists(), "Output directory exists."

    def test_locus_breaker_step_summary_stats(self, session: Session) -> None:
        """Test locus-breaker clumping step writes study loci annotated with their summary statistics."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir, "GCST005523_chr18_lbc")
            LocusBreakerClumpingStep(
                session=session,
                summary_statistics_input_path="tests/gentropy/data_samples/sumstats_sample",
                clumped_study_locus_output_path=str(output_path),
                lbc_baseline_pvalue=1e-5,
                lbc_distance_cutoff=250_000,
                lbc_pvalue_threshold=1e-8,
                lbc_flanking_distance=100_000,
                large_loci_size=1_500_000,
                wbc_clump_distance=500_000,
                wbc_pvalue_threshold=1e-8,
                collect_locus=True,
            )
            loci = AnnotatedStudyLocus.from_parquet(session, str(output_path))
            assert loci.df.count() > 0, "Locus-breaker clumping found no locus."
