"""Test PICS finemapping."""

from __future__ import annotations

from otg.dataset.study_locus import StudyLocus
from otg.method.pics import PICS


def test_pics(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)
