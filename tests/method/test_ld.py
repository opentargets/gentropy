"""Test LD annotation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from otg.dataset.study_locus import StudyLocus
from otg.method.ld import LDAnnotatorGnomad, LDclumping

if TYPE_CHECKING:
    from otg.dataset.ld_index import LDIndex
    from otg.dataset.variant_annotation import VariantAnnotation


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(LDclumping.clump(mock_study_locus), StudyLocus)


def test_variant_coordinates_in_ldindex(
    mock_variant_annotation: VariantAnnotation, mock_ld_index: LDIndex
) -> None:
    """Test function that finds the indices of a particular set of variants in a LDIndex to query it afterwards."""
    variants_df = mock_variant_annotation.df.select(
        "chromosome", "position", "referenceAllele", "alternateAllele"
    )
    variants_w_indices_df = LDAnnotatorGnomad._variant_coordinates_in_ldindex(
        variants_df, mock_ld_index
    )
    expected_cols = ["chromosome", "idx", "start_idx", "stop_idx", "i"]
    assert set(variants_w_indices_df.columns) == set(expected_cols)
