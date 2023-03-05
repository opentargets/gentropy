"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from xgboost import plot_importance as xgb_plot_importance
from xgboost.spark import SparkXGBRegressor

if TYPE_CHECKING:
    from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix


class LocusToGeneModel(SparkXGBRegressor):
    """Wrapper for the Locus to Gene classifier."""

    label_col: str
    missing: float
    n_estimators: int

    def __init__(self: LocusToGeneModel) -> None:
        """TBC."""
        super().__init__()

    def save(self: LocusToGeneModel, path: str) -> None:
        """TBC."""
        pass

    def evaluate(self: LocusToGeneModel, test_df: L2GFeatureMatrix) -> None:
        """TBC."""
        pass

    def plot_importance(self: LocusToGeneModel) -> None:
        """TBC."""
        xgb_plot_importance(self)  # FIXME: What is the attribute that stores the model?


@dataclass
class LocusToGenePredictor:
    """Predicts what is the most likely causal gene associated with a given locus.

    Expected usage is sth like:
    LocusToGenePredictor.load_model(model_path).predict(annotated_loci_df)
    """

    model: LocusToGeneModel

    @classmethod
    def load_model(
        cls: type[LocusToGenePredictor], model_path: str
    ) -> LocusToGenePredictor:
        """Load a model from a given path."""
        return cls(model=SparkXGBRegressor.load(model_path))

    def predict(
        self: LocusToGenePredictor, test_df: L2GFeatureMatrix
    ) -> L2GFeatureMatrix:  # FIXME: Review
        """Apply the model to a given L2GFeatureMatrix dataset."""
        # model: LocusToGeneModel = hydra.utils.instantiate(cfg.model)

        return self.model.transform(test_df)

    def evaluate(self: LocusToGenePredictor) -> None:
        """TBC."""
        pass


class LocusToGeneTrainer:
    """Modelling of what is the most likely causal gene associated with a given locus."""

    @classmethod
    def train(
        cls: type[LocusToGeneTrainer],
        model: LocusToGeneModel,
        train_df: L2GFeatureMatrix,
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        Args:
            model (LocusToGeneModel): Model to train
            train_df (L2GFeatureMatrix): Training data

        Returns:
            LocusToGeneModel: Trained model
        """
        # TODO: Read config from hydra to parametrise the model
        model.fit(train_df)
        return model

    def k_fold_cross_validation(self: LocusToGeneTrainer) -> None:
        """TBC."""
        # See this demo: https://xgboost.readthedocs.io/en/stable/python/examples/cross_validation.html#sphx-glr-python-examples-cross-validation-py
        pass

    def evaluate(self: LocusToGeneTrainer) -> None:
        """TBC."""
        """
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        evaluator.setRawPredictionCol("prediction")
        run = wandb.init(
            project="otg-l2g", job_type="binary-class-xgboost-logloss", save_code=False
        )  # TODO: parametrise all this
        wand_evaluator = WandbEvaluator(sparkMlEvaluator=evaluator, run=run) # FIXME: this class is here https://github.com/timsetsfire/wandb-spark/blob/main/wandb/spark/evaluation.py
        wandb_evaluator.setWandbRun(run)
        wandb_evaluator.setMetricPrefix("test/")
        wandb_evaluator.evaluate(dataset)
        run.finish()
        """
