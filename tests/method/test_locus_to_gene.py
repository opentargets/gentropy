"""Test locus-to-gene model training."""

from __future__ import annotations

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder

from otg.dataset.l2g.feature_matrix import L2GFeatureMatrix
from otg.method.locus_to_gene import LocusToGeneModel, LocusToGeneTrainer


@pytest.fixture(scope="module")
def model() -> LocusToGeneModel:
    """Creates an instance of the LocusToGene class."""
    estimator = LogisticRegression(featuresCol="features", labelCol="label")
    return LocusToGeneModel(estimator=estimator, features_list=["distanceTssMean"])


class TestLocusToGeneTrainer:
    """Test the L2GTrainer methods using a logistic regression model as estimation algorithm."""

    def test_cross_validate(
        self: TestLocusToGeneTrainer,
        mock_l2g_feature_matrix: L2GFeatureMatrix,
        model: LocusToGeneModel,
    ) -> None:
        """Test the k-fold cross-validation function with a logistic regression model."""
        param_grid = (
            ParamGridBuilder().addGrid(model.estimator.regParam, [0.1, 0.01]).build()
        )
        best_model = LocusToGeneTrainer.cross_validate(
            model, mock_l2g_feature_matrix, num_folds=2, param_grid=param_grid
        )

        # Check that the best model is trained with the optimal hyperparameters
        assert best_model.estimator.getMaxIter() == 100
        assert best_model.estimator.getRegParam() == 0.0

    def test_train(
        self: TestLocusToGeneTrainer,
        mock_l2g_feature_matrix: L2GFeatureMatrix,
        model: LocusToGeneModel,
    ) -> None:
        """Test the training function with a logistic regression model."""
        trained_model = LocusToGeneTrainer.train(
            mock_l2g_feature_matrix, model, features_list=["distanceTssMean"]
        )

        # Check that `model` is a PipelineModel object and not None
        assert isinstance(trained_model.model, PipelineModel)
