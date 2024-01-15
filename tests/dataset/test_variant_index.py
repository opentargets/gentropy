"""Tests on variant index generation."""
from __future__ import annotations

from typing import TYPE_CHECKING

from oxygen.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from oxygen.dataset.study_locus import StudyLocus
    from oxygen.dataset.variant_annotation import VariantAnnotation


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)


def test_from_variant_annotation(
    mock_variant_annotation: VariantAnnotation, mock_study_locus: StudyLocus
) -> None:
    """Test variant index creation from variant annotation."""
    variant_index = VariantIndex.from_variant_annotation(
        mock_variant_annotation, mock_study_locus
    )
    assert isinstance(variant_index, VariantIndex)
