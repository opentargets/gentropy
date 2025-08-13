"""Tests on L2G trainer."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from xgboost import XGBClassifier

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
        model=XGBClassifier(),
        hyperparameters={"max_depth": 5},
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
    """Test LocusToGeneTrainer.train with cross validation."""
    # Mock simple model
    features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
    l2g_model = LocusToGeneModel(
        model=XGBClassifier(),
        hyperparameters={"max_depth": 5},
        features_list=features_list,
    )
    trainer = LocusToGeneTrainer(
        model=l2g_model,
        feature_matrix=mock_l2g_feature_matrix.fill_na(),
        features_list=features_list,
    )
    trained_model = trainer.train(wandb_run_name=None, cross_validate=True, n_splits=3)
    assert isinstance(trained_model, LocusToGeneModel)


def test_hierarchical_split() -> None:
    """Test LocusToGeneTrainer.hierarchical_split function."""
    df = pd.DataFrame(
        {
            "geneId": ["G1", "G1", "G1", "G2", "G2", "G3", "G4", "G4"],
            "studyLocusId": ["L1", "L1", "L2", "L2", "L3", "L3", "L4", "L4"],
            "goldStandardSet": [1, 0, 1, 1, 0, 1, 0, 0],
            "feature1": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        }
    )

    train_df, test_df = LocusToGeneTrainer.hierarchical_split(
        df, test_size=0.15, verbose=False
    )

    # Check split sizes
    assert (
        len(train_df[train_df["geneId"] == "G4"])
        & len(test_df[test_df["geneId"] == "G4"])
        == 0
    ), (
        "G4 should not be applied to any split because it is not assigned to any positive studyLocus"
    )
    assert len(train_df) + len(test_df) != len(df)
    assert (
        len(set(train_df["studyLocusId"]).intersection(set(test_df["studyLocusId"])))
        == 0
    ), "Data leakage detected! Overlapping studyLocusIds between splits."
    assert len(set(train_df["geneId"]).intersection(set(test_df["geneId"]))) > 0, (
        "G1 is not present in both splits"
    )
    assert len(train_df[train_df["goldStandardSet"] == 0]) > 0, (
        "No negatives in train_df"
    )
    assert len(test_df[test_df["goldStandardSet"] == 0]) > 0, "No negatives in test_df"
    assert train_df.shape[1] == df.shape[1], "Columns are missing in train_df"
    assert test_df.shape[1] == df.shape[1], "Columns are missing in test_df"
