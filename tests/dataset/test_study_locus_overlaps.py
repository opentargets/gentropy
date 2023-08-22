"""Test colocalisation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from otg.dataset.study_index import StudyIndex
    from otg.dataset.study_locus import StudyLocus


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_study_locus_overlap_from_associations(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(mock_study_locus, mock_study_index)
    assert isinstance(overlaps, StudyLocusOverlap)
