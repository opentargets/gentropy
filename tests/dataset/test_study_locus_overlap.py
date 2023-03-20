"""Test study locus overlap dataset."""
from __future__ import annotations

from otg.dataset.study_locus_overlap import StudyLocusOverlap


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test study locus overlap creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)
