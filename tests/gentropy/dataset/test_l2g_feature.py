"""Test L2G feature generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from gentropy.dataset.l2g_feature import (
    EQtlColocClppMaximumFeature,
    EQtlColocH4MaximumFeature,
    L2GFeature,
    PQtlColocClppMaximumFeature,
    PQtlColocH4MaximumFeature,
    SQtlColocClppMaximumFeature,
    SQtlColocH4MaximumFeature,
    TuQtlColocClppMaximumFeature,
    TuQtlColocH4MaximumFeature,
)
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from gentropy.dataset.colocalisation import Colocalisation
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import StudyLocus


@pytest.mark.parametrize(
    "feature_class",
    [
        EQtlColocH4MaximumFeature,
        PQtlColocH4MaximumFeature,
        SQtlColocH4MaximumFeature,
        TuQtlColocH4MaximumFeature,
        EQtlColocClppMaximumFeature,
        PQtlColocClppMaximumFeature,
        SQtlColocClppMaximumFeature,
        TuQtlColocClppMaximumFeature,
    ],
)
def test_feature_factory_return_type(
    feature_class: Any,
    mock_study_locus: StudyLocus,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
) -> None:
    """Test that every feature factory returns a L2GFeature dataset."""
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation,
        study_index=mock_study_index,
        study_locus=mock_study_locus,
    )
    feature_dataset = feature_class.compute(
        study_loci_to_annotate=mock_study_locus,
        feature_dependency=loader.get_dependency_by_type(
            feature_class.feature_dependency_type
        ),
    )
    assert isinstance(feature_dataset, L2GFeature)
