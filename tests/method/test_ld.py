"""Test LD annotation."""

from __future__ import annotations

from otg.dataset.study_locus import StudyLocus
from otg.method.ld import LDclumping


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(LDclumping.clump(mock_study_locus), StudyLocus)
