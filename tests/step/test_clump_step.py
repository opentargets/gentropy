"""Test clump step."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.clump import ClumpStep

if TYPE_CHECKING:
    from otg.common.session import Session


class TestClumpStep:
    """Test clump step."""

    def test_clumpstep_summary_stats(
        self: TestClumpStep,
        session: Session,
    ) -> None:
        """Test clump step on summary statistics."""
        ClumpStep(
            session=session,
            input_path="tests/data_samples/GCST005523_chr18.parquet",
            clumped_study_locus_path="GCST005523_chr18_clumped",
        )
