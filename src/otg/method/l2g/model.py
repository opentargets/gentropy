"""Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Type

import wandb
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from wandb.wandb_run import Run

from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.method.l2g.evaluator import WandbEvaluator

if TYPE_CHECKING:
    from pyspark.ml import Transformer
    from pyspark.sql import DataFrame


@dataclass
class LocusToGeneModel:
    """Wrapper for the Locus to Gene classifier."""

    features_list: List[str]
    estimator: Any = None
    pipeline: Pipeline = Pipeline(stages=[])
    model: Optional[PipelineModel] = None

    def __post_init__(self: LocusToGeneModel) -> None:
        """Post init that adds the model to the ML pipeline."""
        label_indexer = StringIndexer(
            inputCol="goldStandardSet", outputCol="label", handleInvalid="keep"
        )
        vector_assembler = LocusToGeneModel.features_vector_assembler(
            self.features_list
        )

        self.pipeline = Pipeline(
            stages=[
                label_indexer,
                vector_assembler,
            ]
        )

    def save(self: LocusToGeneModel, path: str) -> None:
        """Saves fitted pipeline model to disk."""
        if self.model is None:
            raise ValueError("Model has not been fitted yet.")
        self.model.write().overwrite().save(path)

    @property
    def classifier(self: LocusToGeneModel) -> Any:
        """Return the model."""
        return self.estimator

    @staticmethod
    def features_vector_assembler(features_cols: List[str]) -> VectorAssembler:
        """Spark transformer to assemble the feature columns into a vector.

        Args:
            features_cols (List[str]): List of feature columns to assemble

        Returns:
            VectorAssembler: Spark transformer to assemble the feature columns into a vector

        Examples:
            >>> from pyspark.ml.feature import VectorAssembler
            >>> df = spark.createDataFrame([(5.2, 3.5)], schema="feature_1 FLOAT, feature_2 FLOAT")
            >>> assembler = LocusToGeneModel.features_vector_assembler(["feature_1", "feature_2"])
            >>> assembler.transform(df).show()
            +---------+---------+--------------------+
            |feature_1|feature_2|            features|
            +---------+---------+--------------------+
            |      5.2|      3.5|[5.19999980926513...|
            +---------+---------+--------------------+
            <BLANKLINE>
        """
        return (
            VectorAssembler(handleInvalid="error")
            .setInputCols(features_cols)
            .setOutputCol("features")
        )

    @staticmethod
    def log_to_wandb(
        results: DataFrame,
        binary_evaluator: BinaryClassificationEvaluator,
        multi_evaluator: MulticlassClassificationEvaluator,
        wandb_run: Run,
    ) -> None:
        """Perform evaluation of the model by applying it to a test set and tracking the results with W&B."""
        binary_wandb_evaluator = WandbEvaluator(
            spark_ml_evaluator=binary_evaluator, wandb_run=wandb_run
        )
        binary_wandb_evaluator.evaluate(results)
        multi_wandb_evaluator = WandbEvaluator(
            spark_ml_evaluator=multi_evaluator, wandb_run=wandb_run
        )
        multi_wandb_evaluator.evaluate(results)

    @classmethod
    def load_from_disk(
        cls: Type[LocusToGeneModel], path: str, features_list: List[str]
    ) -> LocusToGeneModel:
        """Load a fitted pipeline model from disk."""
        return cls(model=PipelineModel.load(path), features_list=features_list)

    @classifier.setter  # type: ignore
    def classifier(self: LocusToGeneModel, new_estimator: Any) -> None:
        """Set the model."""
        self.estimator = new_estimator

    def get_param_grid(self: LocusToGeneModel) -> list:
        """Return the parameter grid for the model."""
        return (
            ParamGridBuilder()
            .addGrid(self.estimator.max_depth, [3, 5, 7])
            .addGrid(self.estimator.learning_rate, [0.01, 0.1, 1.0])
            .build()
        )

    def add_pipeline_stage(
        self: LocusToGeneModel, transformer: Transformer
    ) -> LocusToGeneModel:
        """Adds a stage to the L2G pipeline.

        Args:
            transformer (Transformer): Spark transformer to add to the pipeline

        Returns:
            LocusToGeneModel: L2G model with the new transformer

        Examples:
            >>> from pyspark.ml.regression import LinearRegression
            >>> estimator = LinearRegression()
            >>> test_model = LocusToGeneModel(features_list=["a", "b"])
            >>> print(len(test_model.pipeline.getStages()))
            2
            >>> print(len(test_model.add_pipeline_stage(estimator).pipeline.getStages()))
            3
        """
        pipeline_stages = self.pipeline.getStages()
        new_stages = pipeline_stages + [transformer]
        self.pipeline = Pipeline(stages=new_stages)
        return self

    def evaluate(
        self: LocusToGeneModel,
        results: DataFrame,
        hyperparameters: dict,
        wandb_run_name: Optional[str],
    ) -> None:
        """Perform evaluation of the model by applying it to a test set and tracking the results with W&B."""
        binary_evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="rawPrediction", labelCol="label"
        )
        multi_evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction"
        )

        print("Evaluating model...")
        print(
            "... Area under ROC curve:",
            binary_evaluator.evaluate(
                results, {binary_evaluator.metricName: "areaUnderROC"}
            ),
        )
        print(
            "... Area under Precision-Recall curve:",
            binary_evaluator.evaluate(
                results, {binary_evaluator.metricName: "areaUnderPR"}
            ),
        )
        print(
            "... Accuracy:",
            multi_evaluator.evaluate(results, {multi_evaluator.metricName: "accuracy"}),
        )
        print(
            "... F1 score:",
            multi_evaluator.evaluate(results, {multi_evaluator.metricName: "f1"}),
        )

        if wandb_run_name:
            print("Logging to W&B...")
            run = wandb.init(
                project="otg_l2g", config=hyperparameters, name=wandb_run_name
            )
            if isinstance(run, Run):
                LocusToGeneModel.log_to_wandb(
                    results, binary_evaluator, multi_evaluator, run
                )
                run.finish()

    def plot_importance(self: LocusToGeneModel) -> None:
        """Plot the feature importance of the model."""
        # xgb_plot_importance(self)  # FIXME: What is the attribute that stores the model?

    def fit(
        self: LocusToGeneModel,
        feature_matrix: L2GFeatureMatrix,
    ) -> LocusToGeneModel:
        """Fit the pipeline to the feature matrix dataframe."""
        self.model = self.pipeline.fit(feature_matrix.df)
        return self

    def predict(
        self: LocusToGeneModel,
        feature_matrix: L2GFeatureMatrix,
    ) -> DataFrame:
        """Apply the model to a given feature matrix dataframe. The feature matrix needs to be preprocessed first."""
        if not self.model:
            raise ValueError("Model not fitted yet. `fit()` has to be called first.")
        return self.model.transform(feature_matrix.df)
