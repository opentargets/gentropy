"""Test locus-to-gene model training."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder

from otg.dataset.l2g.feature_matrix import L2GFeatureMatrix
from otg.method.locus_to_gene import LocusToGeneModel, LocusToGeneTrainer

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestLocusToGeneTrainer:
    """Test the L2GTrainer methods."""

    def test_cross_validate(self: TestLocusToGeneTrainer, spark: SparkSession) -> None:
        """Test the k-fold cross-validation function with a logistic regression model."""
        df = spark.createDataFrame(
            [
                (1.0, 2.0, "negative"),
                (4.0, 5.0, "positive"),
                (7.0, 8.0, "negative"),
                (10.0, 11.0, "positive"),
            ],
            ["feature_1", "feature_2", "gold_standard_set"],
        )
        data = L2GFeatureMatrix(_df=df)

        estimator = LogisticRegression(featuresCol="features", labelCol="label")
        model = LocusToGeneModel(
            _estimator=estimator, features_list=["feature_1", "feature_2"]
        )
        param_grid = ParamGridBuilder().addGrid(estimator.regParam, [0.1, 0.01]).build()

        best_model = LocusToGeneTrainer.cross_validate(
            model, data, num_folds=2, param_grid=param_grid
        )

        # Check that the best model is trained with the optimal hyperparameters
        assert best_model._estimator.getMaxIter() == 100
        assert best_model._estimator.getRegParam() == 0.0
