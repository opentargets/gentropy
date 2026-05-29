"""Tests for LocusToGeneCrossValidationStep."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from gentropy.l2g import LocusToGeneCrossValidationStep

if TYPE_CHECKING:
    from gentropy.common.session import Session
    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix


FEATURES = ["distanceTssMean", "distanceSentinelTssMinimum"]


@pytest.fixture()
def split_parquets(
    tmp_path: Path, mock_l2g_feature_matrix: L2GFeatureMatrix
) -> tuple[str, str]:
    """Write the mock feature matrix to train and test parquet paths."""
    fm_df = mock_l2g_feature_matrix.fill_na()._df
    train_path = str(tmp_path / "train.parquet")
    test_path = str(tmp_path / "test.parquet")
    fm_df.write.parquet(train_path)
    fm_df.write.parquet(test_path)
    return train_path, test_path


@pytest.fixture()
def split_parquets_int_labels(
    tmp_path: Path, mock_l2g_feature_matrix: L2GFeatureMatrix
) -> tuple[str, str]:
    """Write the mock feature matrix with integer-encoded labels (as produced by the split step)."""
    from pyspark.sql import functions as f

    fm_df = mock_l2g_feature_matrix.fill_na()._df
    label_map = f.create_map(f.lit("negative"), f.lit(0), f.lit("positive"), f.lit(1))
    fm_df = fm_df.withColumn(
        "goldStandardSet",
        f.coalesce(
            label_map[f.col("goldStandardSet")], f.col("goldStandardSet").cast("int")
        ),
    )
    train_path = str(tmp_path / "train_int.parquet")
    test_path = str(tmp_path / "test_int.parquet")
    fm_df.write.parquet(train_path)
    fm_df.write.parquet(test_path)
    return train_path, test_path


class TestLocusToGeneCrossValidationStep:
    """Integration tests for LocusToGeneCrossValidationStep."""

    def test_runs_end_to_end(
        self,
        session: Session,
        split_parquets: tuple[str, str],
    ) -> None:
        """Step completes without error given valid train/test parquets."""
        train_path, test_path = split_parquets
        LocusToGeneCrossValidationStep(
            session=session,
            train_feature_matrix_path=train_path,
            test_feature_matrix_path=test_path,
            hyperparameters={"max_depth": 3},
            features_list=FEATURES,
            n_splits=2,
        )

    def test_features_list_none_auto_detects(
        self,
        session: Session,
        split_parquets: tuple[str, str],
    ) -> None:
        """features_list=None picks up feature columns from the feature matrix."""
        train_path, test_path = split_parquets
        # Should not raise; all non-metadata columns become features
        LocusToGeneCrossValidationStep(
            session=session,
            train_feature_matrix_path=train_path,
            test_feature_matrix_path=test_path,
            hyperparameters={"max_depth": 3},
            features_list=None,
            n_splits=2,
        )

    def test_cv_results_dir_creates_files(
        self,
        session: Session,
        split_parquets: tuple[str, str],
        tmp_path: Path,
    ) -> None:
        """cv_results.json and cv_folds.csv are written when cv_results_dir is set."""
        train_path, test_path = split_parquets
        cv_dir = str(tmp_path / "cv_out")
        LocusToGeneCrossValidationStep(
            session=session,
            train_feature_matrix_path=train_path,
            test_feature_matrix_path=test_path,
            hyperparameters={"max_depth": 3},
            features_list=FEATURES,
            n_splits=2,
            cv_results_dir=cv_dir,
        )
        assert (Path(cv_dir) / "cv_results.json").exists()
        assert (Path(cv_dir) / "cv_folds.csv").exists()

    def test_cv_results_json_structure(
        self,
        session: Session,
        split_parquets: tuple[str, str],
        tmp_path: Path,
    ) -> None:
        """cv_results.json contains n_splits, n_configs, and per-fold metrics."""
        train_path, test_path = split_parquets
        cv_dir = str(tmp_path / "cv_struct")
        LocusToGeneCrossValidationStep(
            session=session,
            train_feature_matrix_path=train_path,
            test_feature_matrix_path=test_path,
            hyperparameters={"max_depth": 3},
            features_list=FEATURES,
            n_splits=2,
            cv_results_dir=cv_dir,
        )
        data = json.loads((Path(cv_dir) / "cv_results.json").read_text())
        assert data["n_splits"] == 2
        assert data["n_configs"] == 1
        assert len(data["configs"]) == 1
        # n_splits fold rows + 1 holdout row
        assert len(data["configs"][0]["fold_metrics"]) == 3

    def test_integer_encoded_labels_accepted(
        self,
        session: Session,
        split_parquets_int_labels: tuple[str, str],
    ) -> None:
        """Step handles goldStandardSet encoded as 0/1 integers (output of the split step)."""
        train_path, test_path = split_parquets_int_labels
        LocusToGeneCrossValidationStep(
            session=session,
            train_feature_matrix_path=train_path,
            test_feature_matrix_path=test_path,
            hyperparameters={"max_depth": 3},
            features_list=FEATURES,
            n_splits=2,
        )

    def test_hyperparameter_grid_evaluates_all_configs(
        self,
        session: Session,
        split_parquets: tuple[str, str],
        tmp_path: Path,
    ) -> None:
        """Every config in hyperparameter_grid is evaluated and recorded."""
        train_path, test_path = split_parquets
        cv_dir = str(tmp_path / "cv_grid")
        LocusToGeneCrossValidationStep(
            session=session,
            train_feature_matrix_path=train_path,
            test_feature_matrix_path=test_path,
            hyperparameters={"max_depth": 3},
            features_list=FEATURES,
            n_splits=2,
            hyperparameter_grid={"max_depth": {"values": [3, 5]}},
            cv_results_dir=cv_dir,
        )
        data = json.loads((Path(cv_dir) / "cv_results.json").read_text())
        assert data["n_configs"] == 2
