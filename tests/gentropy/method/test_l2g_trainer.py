"""Tests on L2G trainer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

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


def test_train(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test LocusToGeneTrainer.train produces a fitted model."""
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
    trained_model = trainer.train(wandb_run_name=None)
    assert isinstance(trained_model, LocusToGeneModel)


def test_cross_validate_direct(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test LocusToGeneTrainer.cross_validate runs without error and sets cv metrics."""
    features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
    filled_fm = mock_l2g_feature_matrix.fill_na()
    l2g_model = LocusToGeneModel(
        model=XGBClassifier(),
        hyperparameters={"max_depth": 5},
        features_list=features_list,
    )
    trainer = LocusToGeneTrainer(
        model=l2g_model,
        feature_matrix=filled_fm,
        features_list=features_list,
    )
    label_encoder = l2g_model.label_encoder
    train_df, _ = filled_fm.generate_train_test_split(
        test_size=0.3,
        verbose=False,
        label_encoder=label_encoder,
        label_col=filled_fm.label_col,
    )
    trainer.train_df = train_df
    trainer.x_train = train_df[features_list].apply(pd.to_numeric).to_numpy()
    trainer.y_train = train_df[filled_fm.label_col].apply(pd.to_numeric).to_numpy()
    trainer.cross_validate(n_splits=3)  # should not raise


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
    trained_model = trainer.train(wandb_run_name=None, train_on_full_dataset=True)
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
    with patch.object(trainer, "log_to_wandb", side_effect=wandb_run_names.append):
        trained_model = trainer.train(
            wandb_run_name="unit-test",
            train_on_full_dataset=True,
        )

    assert isinstance(trained_model, LocusToGeneModel)
    assert wandb_run_names == ["unit-test-holdout", "unit-test-full-dataset"]


def test_train_with_presplit_data(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Trainer uses presplit DataFrames directly and skips generate_train_test_split."""
    features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
    l2g_model = LocusToGeneModel(
        model=XGBClassifier(),
        hyperparameters={"max_depth": 5},
        features_list=features_list,
    )
    filled_fm = mock_l2g_feature_matrix.fill_na()
    presplit_train_df, presplit_test_df = filled_fm.generate_train_test_split(
        test_size=0.3,
        verbose=False,
        label_encoder=l2g_model.label_encoder,
        label_col=filled_fm.label_col,
    )
    trainer = LocusToGeneTrainer(
        model=l2g_model,
        feature_matrix=filled_fm,
        features_list=features_list,
    )
    trained_model = trainer.train(
        wandb_run_name=None,
        presplit_train_df=presplit_train_df,
        presplit_test_df=presplit_test_df,
    )
    assert isinstance(trained_model, LocusToGeneModel)
    assert trainer.train_df is presplit_train_df, (
        "Presplit train DataFrame was not used directly"
    )
    assert trainer.test_df is presplit_test_df, (
        "Presplit test DataFrame was not used directly"
    )


class TestExpandGrid:
    """Unit tests for LocusToGeneTrainer._expand_grid."""

    def test_single_param(self) -> None:
        """Single-parameter grid returns one dict per value."""
        grid = {"max_depth": {"values": [3, 5, 7]}}
        result = LocusToGeneTrainer._expand_grid(grid)
        assert result == [{"max_depth": 3}, {"max_depth": 5}, {"max_depth": 7}]

    def test_cartesian_product(self) -> None:
        """Two parameters produce the full Cartesian product."""
        grid = {
            "max_depth": {"values": [3, 5]},
            "eta": {"values": [0.05, 0.1]},
        }
        result = LocusToGeneTrainer._expand_grid(grid)
        assert len(result) == 4
        assert {"max_depth": 3, "eta": 0.05} in result
        assert {"max_depth": 5, "eta": 0.1} in result

    def test_missing_values_key_raises(self) -> None:
        """Grid entry without a 'values' key raises ValueError."""
        with pytest.raises(ValueError, match="values"):
            LocusToGeneTrainer._expand_grid({"max_depth": {"bad_key": [3]}})


class TestWriteCvFiles:
    """Unit tests for LocusToGeneTrainer._write_cv_files."""

    def test_creates_json_and_csv(self, tmp_path: Path) -> None:
        """Both cv_results.json and cv_folds.csv are created."""
        summaries = [
            {
                "config_id": 0,
                "hyperparameters": {"max_depth": 3},
                "fold_metrics": [{"fold": 1, "areaUnderROC": 0.8}],
                "mean_metrics": {"areaUnderROC": 0.8},
                "std_metrics": {"areaUnderROC": 0.0},
            }
        ]
        LocusToGeneTrainer._write_cv_files(summaries, tmp_path, n_splits=2)
        assert (tmp_path / "cv_results.json").exists()
        assert (tmp_path / "cv_folds.csv").exists()

    def test_json_n_configs_matches_summaries(self, tmp_path: Path) -> None:
        """n_configs in JSON equals the number of summary dicts passed in."""
        summaries = [
            {
                "config_id": i,
                "hyperparameters": {"max_depth": i + 3},
                "fold_metrics": [{"fold": 1, "areaUnderROC": 0.7}],
                "mean_metrics": {"areaUnderROC": 0.7},
                "std_metrics": {"areaUnderROC": 0.0},
            }
            for i in range(3)
        ]
        LocusToGeneTrainer._write_cv_files(summaries, tmp_path, n_splits=2)
        data = json.loads((tmp_path / "cv_results.json").read_text())
        assert data["n_configs"] == 3
        assert data["n_splits"] == 2


class TestCrossValidateWithCvResultsDir:
    """Tests for cross_validate writing results to a local directory."""

    def test_cv_results_dir_writes_files(
        self, tmp_path: Path, mock_l2g_feature_matrix: L2GFeatureMatrix
    ) -> None:
        """cv_results.json and cv_folds.csv appear in cv_results_dir."""
        features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
        filled_fm = mock_l2g_feature_matrix.fill_na()
        l2g_model = LocusToGeneModel(
            model=XGBClassifier(),
            hyperparameters={"max_depth": 3},
            features_list=features_list,
        )
        trainer = LocusToGeneTrainer(
            model=l2g_model,
            feature_matrix=filled_fm,
            features_list=features_list,
        )
        label_encoder = l2g_model.label_encoder
        train_df, _ = filled_fm.generate_train_test_split(
            test_size=0.3,
            verbose=False,
            label_encoder=label_encoder,
            label_col=filled_fm.label_col,
        )
        trainer.train_df = train_df
        trainer.x_train = train_df[features_list].apply(pd.to_numeric).to_numpy()
        trainer.y_train = train_df[filled_fm.label_col].apply(pd.to_numeric).to_numpy()

        cv_dir = str(tmp_path / "cv_out")
        trainer.cross_validate(n_splits=2, cv_results_dir=cv_dir)

        assert (Path(cv_dir) / "cv_results.json").exists()
        assert (Path(cv_dir) / "cv_folds.csv").exists()

    def test_hyperparameter_grid_with_cv_results_dir(
        self, tmp_path: Path, mock_l2g_feature_matrix: L2GFeatureMatrix
    ) -> None:
        """Each config in the grid gets its own sub-directory of plots."""
        features_list = ["distanceTssMean", "distanceSentinelTssMinimum"]
        filled_fm = mock_l2g_feature_matrix.fill_na()
        l2g_model = LocusToGeneModel(
            model=XGBClassifier(),
            hyperparameters={"max_depth": 3},
            features_list=features_list,
        )
        trainer = LocusToGeneTrainer(
            model=l2g_model,
            feature_matrix=filled_fm,
            features_list=features_list,
        )
        label_encoder = l2g_model.label_encoder
        train_df, _ = filled_fm.generate_train_test_split(
            test_size=0.3,
            verbose=False,
            label_encoder=label_encoder,
            label_col=filled_fm.label_col,
        )
        trainer.train_df = train_df
        trainer.x_train = train_df[features_list].apply(pd.to_numeric).to_numpy()
        trainer.y_train = train_df[filled_fm.label_col].apply(pd.to_numeric).to_numpy()

        cv_dir = str(tmp_path / "cv_grid")
        trainer.cross_validate(
            n_splits=2,
            parameter_grid={"max_depth": {"values": [3, 5]}},
            cv_results_dir=cv_dir,
        )

        data = json.loads((Path(cv_dir) / "cv_results.json").read_text())
        assert data["n_configs"] == 2
        assert (Path(cv_dir) / "config_0").is_dir()
        assert (Path(cv_dir) / "config_1").is_dir()


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
