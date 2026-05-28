"""Tests on L2G trainer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pytest
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
        "TP": 2,
        "FP": 0,
        "TN": 2,
        "FN": 0,
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


def test_train_on_full_dataset(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test that train_on_full_dataset retrains on the combined train+test set.

    The final model should have seen more samples than the training split alone,
    while reported metrics still come from the honest held-out evaluation.
    """
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
    trained_model = trainer.train(
        wandb_run_name=None, cross_validate=False, train_on_full_dataset=True
    )
    assert isinstance(trained_model, LocusToGeneModel)
    # After full-dataset retrain x_train covers exactly train+test rows
    assert trainer.train_df is not None
    assert trainer.test_df is not None
    assert trainer.x_train is not None
    assert trainer.x_test is not None
    assert (
        trainer.x_train.shape[0] == trainer.train_df.shape[0] + trainer.test_df.shape[0]
    )
    assert trainer.x_test.shape[0] == trainer.test_df.shape[0]


def test_train_on_full_dataset_logs_second_wandb_run(
    mock_l2g_feature_matrix: L2GFeatureMatrix,
) -> None:
    """Test that W&B logging includes a dedicated run for full-dataset retraining."""
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
    wandb_run_names: list[str] = []
    trainer.log_to_wandb = wandb_run_names.append  # type: ignore[method-assign, assignment]

    trained_model = trainer.train(
        wandb_run_name="unit-test",
        cross_validate=False,
        train_on_full_dataset=True,
    )

    assert isinstance(trained_model, LocusToGeneModel)
    assert wandb_run_names == ["unit-test-holdout", "unit-test-full-dataset"]


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


def test_expand_grid_single_param() -> None:
    """Single-param grid with N values yields N configs."""
    grid = {"max_depth": {"values": [3, 6, 9]}}
    result = LocusToGeneTrainer._expand_grid(grid)
    assert result == [{"max_depth": 3}, {"max_depth": 6}, {"max_depth": 9}]


def test_expand_grid_cartesian_product() -> None:
    """Two-param grid yields the full cartesian product."""
    grid = {"max_depth": {"values": [3, 6]}, "n_estimators": {"values": [100, 200]}}
    result = LocusToGeneTrainer._expand_grid(grid)
    assert len(result) == 4
    assert {"max_depth": 3, "n_estimators": 100} in result
    assert {"max_depth": 6, "n_estimators": 200} in result


def test_expand_grid_missing_values_key_raises() -> None:
    """Grid entry without 'values' key raises a descriptive ValueError."""
    with pytest.raises(ValueError, match="max_depth"):
        LocusToGeneTrainer._expand_grid({"max_depth": {"value": 3}})


def test_cv_results_dir_writes_expected_files(
    mock_l2g_feature_matrix: L2GFeatureMatrix,
    tmp_path: Path,
) -> None:
    """cv_results_dir mode writes cv_results.json, cv_folds.csv, and per-config plots."""
    features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
    l2g_model = LocusToGeneModel(
        model=XGBClassifier(),
        hyperparameters={"max_depth": 3},
        features_list=features_list,
    )
    trainer = LocusToGeneTrainer(
        model=l2g_model,
        feature_matrix=mock_l2g_feature_matrix.fill_na(),
        features_list=features_list,
    )
    trainer.train(
        wandb_run_name=None,
        cross_validate=True,
        n_splits=2,
        hyperparameter_grid={"max_depth": {"values": [3, 6]}},
        cv_results_dir=str(tmp_path),
    )

    assert (tmp_path / "cv_results.json").exists()
    assert (tmp_path / "cv_folds.csv").exists()
    assert (tmp_path / "config_0" / "roc.png").exists()
    assert (tmp_path / "config_1" / "roc.png").exists()

    summary = json.loads((tmp_path / "cv_results.json").read_text())
    assert summary["n_configs"] == 2
    assert summary["n_splits"] == 2
    # Metrics must be plain Python floats, not NumPy scalars (JSON round-trip check)
    for cfg in summary["configs"]:
        for fold in cfg["fold_metrics"]:
            for v in fold.values():
                assert isinstance(v, (int, float)), (
                    f"Non-serialisable metric value: {v!r}"
                )
