"""Test locus-to-gene model training."""

from __future__ import annotations

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.tuning import ParamGridBuilder
from xgboost.spark import SparkXGBClassifier

from otg.dataset.l2g.feature_matrix import L2GFeatureMatrix
from otg.method.locus_to_gene import LocusToGeneModel, LocusToGeneTrainer


@pytest.fixture(scope="module")
def model() -> LocusToGeneModel:
    """Creates an instance of the LocusToGene class."""
    estimator = SparkXGBClassifier(
        eval_metric="logloss",
        features_col="features",
        label_col="label",
        max_depth=5,
    )
    return LocusToGeneModel(estimator=estimator, features_list=["distanceTssMean"])


class TestLocusToGeneTrainer:
    """Test the L2GTrainer methods using a logistic regression model as estimation algorithm."""

    def test_cross_validate(
        self: TestLocusToGeneTrainer,
        mock_l2g_feature_matrix: L2GFeatureMatrix,
        model: LocusToGeneModel,
    ) -> None:
        """Test the k-fold cross-validation function."""
        param_grid = (
            ParamGridBuilder()
            .addGrid(model.estimator.learning_rate, [0.1, 0.01])
            .build()
        )
        best_model = LocusToGeneTrainer.cross_validate(
            model, mock_l2g_feature_matrix.fill_na(), num_folds=2, param_grid=param_grid
        )
        assert isinstance(
            best_model, LocusToGeneModel
        ), "Unexpected model type returned from cross_validate"
        # Check that the best model's hyperparameters are among those in the param_grid
        assert best_model.model.getOrDefault("learning_rate") in [
            0.1,
            0.01,
        ], "Unexpected learning rate in the best model"

    def test_train(
        self: TestLocusToGeneTrainer,
        mock_l2g_feature_matrix: L2GFeatureMatrix,
        model: LocusToGeneModel,
    ) -> None:
        """Test the training function."""
        trained_model = LocusToGeneTrainer.train(
            mock_l2g_feature_matrix.fill_na(), model, features_list=["distanceTssMean"]
        )
        # Check that `model` is a PipelineModel object and not None
        assert isinstance(
            trained_model.model, PipelineModel
        ), "Model is not a PipelineModel object."
