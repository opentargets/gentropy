"""Tests on L2G datasets."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.dataset.l2g_prediction import L2GPrediction

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def test_feature_matrix(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test L2G Feature Matrix creation with mock data."""
    assert isinstance(mock_l2g_feature_matrix, L2GFeatureMatrix)


def test_gold_standard(mock_l2g_gold_standard: L2GFeatureMatrix) -> None:
    """Test L2G gold standard creation with mock data."""
    assert isinstance(mock_l2g_gold_standard, L2GGoldStandard)


def test_process_gene_interactions(sample_otp_interactions: DataFrame) -> None:
    """Tests processing of gene interactions from OTP."""
    expected_cols = ["geneIdA", "geneIdB", "score"]
    observed_df = L2GGoldStandard.process_gene_interactions(sample_otp_interactions)
    assert (
        observed_df.columns == expected_cols
    ), "Gene interactions has a different schema."


def test_predictions(mock_l2g_predictions: L2GFeatureMatrix) -> None:
    """Test L2G predictions creation with mock data."""
    assert isinstance(mock_l2g_predictions, L2GPrediction)
