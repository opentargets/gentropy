"""Test colocalisation methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.method.colocalisation import Coloc, ECaviar

if TYPE_CHECKING:
    from gentropy.dataset.study_locus_overlap import StudyLocusOverlap


def test_coloc(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test coloc."""
    assert isinstance(Coloc.colocalise(mock_study_locus_overlap), Colocalisation)


def test_ecaviar(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test eCAVIAR."""
    assert isinstance(ECaviar.colocalise(mock_study_locus_overlap), Colocalisation)
