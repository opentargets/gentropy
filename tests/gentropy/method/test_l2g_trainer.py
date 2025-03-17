"""Tests on L2G trainer."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier

from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.method.l2g.trainer import LocusToGeneTrainer

if TYPE_CHECKING:
    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix


def test_evaluate_perfect_predictions() -> None:
    """Test function with perfect predictions."""
    y_true = np.array([0, 1, 0, 1])
    y_pred = np.array([0, 1, 0, 1])
    y_pred_proba = np.array(
        [
            [0.9, 0.1],
            [0.1, 0.9],
            [0.9, 0.1],
            [0.1, 0.9],
        ]
    )

    metrics = LocusToGeneTrainer.evaluate(y_true, y_pred, y_pred_proba)
    expected = {
        "areaUnderROC": 1.0,
        "averagePrecision": 1.0,
        "accuracy": 1.0,
        "weightedPrecision": 1.0,
        "weightedRecall": 1.0,
        "f1": 1.0,
    }
    assert metrics == expected


def test_train_no_cross_validation(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test LocusToGeneTrainer.train without cross validation."""
    # Mock simple model
    features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
    l2g_model = LocusToGeneModel(
        model=GradientBoostingClassifier(),
        hyperparameters={"random_state": 42, "loss": "log_loss"},
        features_list=features_list,
    )
    trainer = LocusToGeneTrainer(
        model=l2g_model,
        feature_matrix=mock_l2g_feature_matrix.fill_na(),
        features_list=features_list,
    )
    trained_model = trainer.train(wandb_run_name=None, cross_validate=False)
    assert isinstance(trained_model, LocusToGeneModel)


def test_train_cross_validation(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test LocusToGeneTrainer.train without cross validation."""
    # Mock simple model
    features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
    l2g_model = LocusToGeneModel(
        model=GradientBoostingClassifier(),
        hyperparameters={"random_state": 42, "loss": "log_loss"},
        features_list=features_list,
    )
    trainer = LocusToGeneTrainer(
        model=l2g_model,
        feature_matrix=mock_l2g_feature_matrix.fill_na(),
        features_list=features_list,
    )
    trained_model = trainer.train(wandb_run_name=None, cross_validate=True)
    assert isinstance(trained_model, LocusToGeneModel)
