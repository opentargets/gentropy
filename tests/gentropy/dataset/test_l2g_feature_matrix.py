"""Test L2G feature matrix methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from gentropy.dataset.colocalisation import Colocalisation
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import StudyLocus
    from gentropy.l2g.l2g_gold_standard import L2GGoldStandard


def test_from_features_list_study_locus(
    mock_study_locus: StudyLocus,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
) -> None:
    """Test building feature matrix for a SL with the eQtlColocH4Maximum feature."""
    features_list = ["eQtlColocH4Maximum"]
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation, study_index=mock_study_index
    )
    fm = L2GFeatureMatrix.from_features_list(mock_study_locus, features_list, loader)
    for feature in features_list:
        assert (
            feature in fm._df.columns
        ), f"Feature {feature} not found in feature matrix."


def test_from_features_list_gold_standard(
    mock_l2g_gold_standard: L2GGoldStandard,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
) -> None:
    """Test building feature matrix for a SL with the eQtlColocH4Maximum feature."""
    features_list = ["eQtlColocH4Maximum"]
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation, study_index=mock_study_index
    )
    fm = L2GFeatureMatrix.from_features_list(
        mock_l2g_gold_standard, features_list, loader
    )
    for feature in features_list:
        assert (
            feature in fm._df.columns
        ), f"Feature {feature} not found in feature matrix."
