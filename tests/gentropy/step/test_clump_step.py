"""Test clump step."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

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
