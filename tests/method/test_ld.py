"""Test LD annotation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from numpy import array as nparray

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


def test_variants_in_ld_in_gnomad_pop(
    mock_variant_annotation: VariantAnnotation, mock_ld_index: LDIndex
) -> None:
    """Test function that annotates which variants are in LD in a particular gnomad population."""
    r = nparray(
        [
            [1.0, 0.8, 0.7, 0.2],
            [0.8, 1.0, 0.6, 0.1],
            [0.7, 0.6, 1.0, 0.3],
            [0.2, 0.1, 0.3, 1.0],
        ]
    )
    from hail.linalg import BlockMatrix

    mock_ld_matrix = BlockMatrix.from_numpy(r)
    variants_df = mock_variant_annotation.df.select(
        "chromosome", "position", "referenceAllele", "alternateAllele"
    )
    expanded_variants_df = LDAnnotatorGnomad.variants_in_ld_in_gnomad_pop(
        variants_df, mock_ld_matrix, mock_ld_index, 0.7
    )
    print(expanded_variants_df.columns)
