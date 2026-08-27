"""Tests for the reusable LDSC-CTS primitives."""

from __future__ import annotations

import numpy as np
import pytest
from pyspark.sql import Row

from gentropy.method.ldsc import infer_ld_ancestry, run_ldsc_cts_from_arrays
from gentropy.method.ldsc.cell_type_annotation import compute_annotation_ld_scores


def test_infer_ld_ancestry_aggregates_relative_sample_size() -> None:
    """Duplicate population records contribute to the plurality total."""
    assert infer_ld_ancestry(
        [
            {"ldPopulation": "eas", "relativeSampleSize": 0.35},
            {"ldPopulation": "nfe", "relativeSampleSize": 0.40},
            {"ldPopulation": "eas", "relativeSampleSize": 0.30},
        ]
    ) == "eas"


def test_infer_ld_ancestry_accepts_one_population() -> None:
    """A single recognised population is selected directly."""
    assert infer_ld_ancestry(
        [{"ldPopulation": "nfe", "relativeSampleSize": 1.0}]
    ) == "nfe"


def test_infer_ld_ancestry_selects_largest_aggregate() -> None:
    """The plurality winner is selected independently of input ordering."""
    assert infer_ld_ancestry(
        [
            {"ldPopulation": "afr", "relativeSampleSize": 0.2},
            {"ldPopulation": "eas", "relativeSampleSize": 0.7},
            {"ldPopulation": "nfe", "relativeSampleSize": 0.3},
        ]
    ) == "eas"


def test_infer_ld_ancestry_tie_prefers_nfe() -> None:
    """The tie rule is deterministic and retains the existing NFE preference."""
    assert infer_ld_ancestry(
        [
            {"ldPopulation": "eas", "relativeSampleSize": 0.5},
            {"ldPopulation": "nfe", "relativeSampleSize": 0.5},
        ]
    ) == "nfe"


def test_infer_ld_ancestry_tie_uses_sorted_population() -> None:
    """Ties without NFE use the canonical lexical tie-breaker."""
    assert infer_ld_ancestry(
        [
            {"ldPopulation": "eas", "relativeSampleSize": 0.5},
            {"ldPopulation": "afr", "relativeSampleSize": 0.5},
        ]
    ) == "afr"


def test_infer_ld_ancestry_rejects_unusable_structure() -> None:
    """Empty or unrecognised study metadata cannot select a reference."""
    with pytest.raises(ValueError):
        infer_ld_ancestry([{"ldPopulation": "unknown", "relativeSampleSize": 1.0}])
    with pytest.raises(ValueError):
        infer_ld_ancestry([{"ldPopulation": "nfe", "relativeSampleSize": "not-a-number"}])
    with pytest.raises(ValueError):
        infer_ld_ancestry([{"ldPopulation": "nfe", "relativeSampleSize": 0.0}])
    with pytest.raises(TypeError):
        infer_ld_ancestry({"ldPopulation": "nfe", "relativeSampleSize": 1.0})


def test_flat_edges_preserve_tag_contributions_and_self_terms(spark) -> None:
    """Restricting scored variants does not remove tag contributions."""
    edges = spark.createDataFrame(
        [
            Row(variantId="1_100_A_G", tagVariantId="1_200_C_T", r=0.5),
            Row(variantId="1_200_C_T", tagVariantId="1_300_G_A", r=0.25),
        ]
    )
    annotations = spark.createDataFrame(
        [
            Row(variantId="1_200_C_T", annotation="cell", annotationValue=2.0),
            Row(variantId="1_300_G_A", annotation="cell", annotationValue=3.0),
        ]
    )
    scored = spark.createDataFrame([Row(variantId="1_100_A_G")])
    scores, m_annot = compute_annotation_ld_scores(annotations, edges, scored)
    assert scores.collect()[0]["ldScore"] == pytest.approx(0.5)
    assert m_annot.collect()[0]["M"] == pytest.approx(5.0)


def test_flat_edges_restore_reverse_rows_for_scored_tag_variants(spark) -> None:
    """A scored tag receives contributions from both sides of a flat edge."""
    edges = spark.createDataFrame(
        [
            Row(variantId="1_100_A_G", tagVariantId="1_200_C_T", r=0.5),
            Row(variantId="1_200_C_T", tagVariantId="1_300_G_A", r=0.25),
        ]
    )
    annotations = spark.createDataFrame(
        [
            Row(variantId="1_100_A_G", annotation="cell", annotationValue=1.0),
            Row(variantId="1_200_C_T", annotation="cell", annotationValue=2.0),
            Row(variantId="1_300_G_A", annotation="cell", annotationValue=3.0),
        ]
    )
    scored = spark.createDataFrame([Row(variantId="1_200_C_T")])
    scores, _ = compute_annotation_ld_scores(annotations, edges, scored)
    assert scores.collect()[0]["ldScore"] == pytest.approx(2.4375)


def test_cts_array_wrapper_validates_dimensions() -> None:
    """The statistical primitive rejects inconsistent aligned arrays."""
    with pytest.raises(ValueError, match="share the same number"):
        run_ldsc_cts_from_arrays(
            beta=np.ones(3),
            se=np.ones(2),
            N=np.ones(3),
            ref_ld=np.ones((3, 1)),
            w_ld=np.ones(3),
            M_annot=np.ones(1),
        )
