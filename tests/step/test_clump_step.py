"""Test clump step."""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from otg.clump import ClumpStep

if TYPE_CHECKING:
    from otg.common.session import Session


class TestClumpStep:
    """Test clump step."""

    def test_clumpstep_summary_stats(self, session: Session) -> None:
        """Test clump step on summary statistics writes results to a temporary directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            clumped_study_locus_path = Path(temp_dir, "GCST005523_chr18_clumped")
            ClumpStep(
                session=session,
                input_path="tests/data_samples/sumstats_sample",
                clumped_study_locus_path=str(clumped_study_locus_path),
            )
            assert Path(clumped_study_locus_path).exists(), "Output directory exists."
