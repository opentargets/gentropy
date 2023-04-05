"""Test locus-to-gene model training."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder

from otg.dataset.l2g.feature_matrix import L2GFeatureMatrix
from otg.method.locus_to_gene import LocusToGeneModel, LocusToGeneTrainer

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def mock_training_data(spark: SparkSession) -> L2GFeatureMatrix:
    """L2GFeatureMatrix object whose dataframe mocks the structure of a feature matrix + the label column."""
    df = spark.createDataFrame(
        [
            (1.0, 2.0, "negative"),
            (4.0, 5.0, "positive"),
            (7.0, 8.0, "negative"),
            (10.0, 11.0, "positive"),
        ],
        ["feature_1", "feature_2", "gold_standard_set"],
    )
    return L2GFeatureMatrix(_df=df)


class TestLocusToGeneTrainer:
    """Test the L2GTrainer methods using a logistic regression model as estimation algorithm."""

    def __init__(self: TestLocusToGeneTrainer) -> None:
        """Constructor for the TestLocusToGeneTrainer class."""
        self.estimator = LogisticRegression(featuresCol="features", labelCol="label")
        self.model = LocusToGeneModel(
            _estimator=self.estimator, features_list=["feature_1", "feature_2"]
        )

    def test_cross_validate(
        self: TestLocusToGeneTrainer,
        mock_training_data: L2GFeatureMatrix,
    ) -> None:
        """Test the k-fold cross-validation function with a logistic regression model."""
        param_grid = (
            ParamGridBuilder().addGrid(self.estimator.regParam, [0.1, 0.01]).build()
        )
        best_model = LocusToGeneTrainer.cross_validate(
            self.model, mock_training_data, num_folds=2, param_grid=param_grid
        )

        # Check that the best model is trained with the optimal hyperparameters
        assert best_model._estimator.getMaxIter() == 100
        assert best_model._estimator.getRegParam() == 0.0

    def test_train(
        self: TestLocusToGeneTrainer,
        mock_training_data: L2GFeatureMatrix,
    ) -> None:
        """Test the training function with a logistic regression model."""
        trained_model = LocusToGeneTrainer.train(mock_training_data, self.model)

        # Check that `model` is a PipelineModel object and not None
        assert isinstance(trained_model.model, PipelineModel)
