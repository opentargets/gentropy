"""Tests for fine-mapping locus-set LD annotation helpers."""

import pytest

from gentropy.fine_mapping_locus_set_ld_annotation import (
    FineMappingLocusSetLDAnnotationStep,
)


def test_normalize_ld_references_accepts_flat_reference_configuration() -> None:
    """Flat ancestry/index/matrix records normalize to PanUKBB labels."""
    references = FineMappingLocusSetLDAnnotationStep._normalize_ld_references(
        [{"ancestry": "nfe", "vi_path": "eur.vi", "bm_path": "eur.bm"}]
    )

    assert references == {
        "EUR": {"ancestry": "EUR", "vi_path": "eur.vi", "bm_path": "eur.bm"}
    }


@pytest.mark.parametrize(
    "references",
    [
        [],
        [{"ancestry": "EUR", "vi_path": "eur.vi"}],
        [
            {"ancestry": "EUR", "vi_path": "eur.vi", "bm_path": "eur.bm"},
            {"ancestry": "nfe", "vi_path": "nfe.vi", "bm_path": "nfe.bm"},
        ],
    ],
)
def test_normalize_ld_references_rejects_invalid_configuration(
    references: list[dict[str, str]],
) -> None:
    """Missing and duplicate ancestry references fail early."""
    with pytest.raises(ValueError):
        FineMappingLocusSetLDAnnotationStep._normalize_ld_references(references)


def test_locus_variant_ids_deduplicates_in_input_order() -> None:
    """Repeated variants in collected locus arrays do not duplicate matrix indices."""
    assert FineMappingLocusSetLDAnnotationStep._locus_variant_ids(
        [
            {"variantId": "1_10_A_C"},
            {"variantId": "1_20_G_T"},
            {"variantId": "1_10_A_C"},
        ]
    ) == ["1_10_A_C", "1_20_G_T"]


def test_validate_reference_coverage_rejects_unregistered_ancestry() -> None:
    """Every observed ancestry must have an index and BlockMatrix reference."""
    with pytest.raises(ValueError, match="AFR"):
        FineMappingLocusSetLDAnnotationStep._validate_reference_coverage(
            ["EUR", "AFR"],
            {"EUR": {"vi_path": "eur.vi", "bm_path": "eur.bm"}},
        )
