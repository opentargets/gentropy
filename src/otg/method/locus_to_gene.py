"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List

import wandb
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from xgboost.spark import SparkXGBClassifier

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix


@dataclass
class LocusToGeneModel:
    """Wrapper for the Locus to Gene classifier."""

    # label_col: str
    # missing: float
    # n_estimators: int

    _model: Any

    def save(self: LocusToGeneModel, path: str) -> None:
        """TBC."""
        pass

    @property
    def model(self: LocusToGeneModel) -> Any:
        """Return the model."""
        return self._model

    def plot_importance(self: LocusToGeneModel) -> None:
        """TBC."""
        # xgb_plot_importance(self)  # FIXME: What is the attribute that stores the model?

    @model.setter  # type: ignore
    def model(self: LocusToGeneModel, new_model: Any) -> None:
        """Set the model."""
        self._model = new_model


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
        return cls(model=SparkXGBClassifier.load(model_path))

    def predict(
        self: LocusToGenePredictor, test_df: L2GFeatureMatrix
    ) -> DataFrame:  # FIXME: Review
        """Apply the model to a given L2GFeatureMatrix dataset."""
        return self.model.model.transform(test_df._df)

    def evaluate(self: LocusToGenePredictor) -> None:
        """TBC."""
        pass


@dataclass
class LocusToGeneTrainer:
    """Modelling of what is the most likely causal gene associated with a given locus."""

    _model: LocusToGeneModel
    train_set: L2GFeatureMatrix

    @staticmethod
    def evaluate(test_set: DataFrame, wandb_project: str = "otg_l2g") -> None:
        """Perform evaluation of the model by tracking ."""
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        evaluator.setRawPredictionCol("prediction")
        run = wandb.init(
            project=wandb_project,
            job_type="binary-class-xgboost-logloss",
            save_code=False,
        )
        wandb_evaluator = wandb.WandbEvaluator(sparkMlEvaluator=evaluator, run=run)
        wandb_evaluator.setWandbRun(run)
        wandb_evaluator.setMetricPrefix("test/")
        wandb_evaluator.evaluate(test_set)
        run.finish()

    @staticmethod
    def features_vector_assembler(features_cols: List[str]) -> VectorAssembler:
        """Spark transformer to assemble the feature columns into a vector."""
        return VectorAssembler().setInputCols(features_cols).setOutputCol("features")

    @classmethod
    def train(
        cls: type[LocusToGeneTrainer],
        model: LocusToGeneModel,
        train_set: L2GFeatureMatrix,
        feature_cols: List[str],
        track: bool,
        **hyperparams: dict,
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        Args:
            model (LocusToGeneModel): Model to fit to the data
            train_set (L2GFeatureMatrix): Training data
            feature_cols (List[str]): List of feature columns to use
            track (bool): Whether to track the training process with wandb
            hyperparams (dict): Hyperparameters to use for the model

        Returns:
            LocusToGeneModel: Trained model
        """
        train, eval = train_set.train_test_split(fraction=0.8)

        pipeline = (
            # Define the stages of the pipeline
            Pipeline(
                stages=[
                    LocusToGeneTrainer.features_vector_assembler(feature_cols),
                    model._model,
                ]
            )
            # Fit the pipeline to the training data
            .fit(train.df)
        )

        if track:
            LocusToGeneTrainer.evaluate(test_set=pipeline.transform(eval))
        return model

    def k_fold_cross_validation(
        self: LocusToGeneTrainer, data: L2GFeatureMatrix
    ) -> None:
        """TBC."""
        # See this demo: https://xgboost.readthedocs.io/en/stable/python/examples/cross_validation.html#sphx-glr-python-examples-cross-validation-py
        feature_cols = data._df.drop("label").columns
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="label")
        data = assembler.transform(data)

        train_data, test_data = data.train_test_split(0.8)

        # Define model
        xgb = SparkXGBClassifier(
            eval_metric="logloss",
            num_round=10,
            num_workers=4,
            featuresCol="features",
            labelCol="label",
        )
        param_grid = (
            ParamGridBuilder()
            .addGrid(xgb.maxDepth, [2, 4, 6])
            .addGrid(xgb.eta, [0.01, 0.1, 0.3])
            .build()
        )

        # Define evaluator - TODO: use my custom one
        evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

        # Define cross validator
        cross_validator = CrossValidator(
            estimator=xgb,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=5,
        )

        # set all stages in the pipeline
        pipeline = Pipeline(stages=[assembler, xgb, cross_validator])
        predictions = pipeline.fit(train_data).transform(test_data)

        evaluator(predictions)
