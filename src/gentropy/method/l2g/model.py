"""Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Type

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from wandb.data_types import Table
from wandb.sdk import init as wandb_init
from wandb.wandb_run import Run
from xgboost.spark.core import SparkXGBClassifierModel

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.evaluator import WandbEvaluator

if TYPE_CHECKING:
    from pyspark.ml import Transformer
    from pyspark.sql import DataFrame


@dataclass
class LocusToGeneModel:
    """Wrapper for the Locus to Gene classifier."""

    features_list: list[str]
    estimator: Any = None
    pipeline: Pipeline = Pipeline(stages=[])
    model: PipelineModel | None = None
    wandb_l2g_project_name: str = "otg_l2g"

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
        """Saves fitted pipeline model to disk.

        Args:
            path (str): Path to save the model to

        Raises:
            ValueError: If the model has not been fitted yet
        """
        if self.model is None:
            raise ValueError("Model has not been fitted yet.")
        self.model.write().overwrite().save(path)

    @property
    def classifier(self: LocusToGeneModel) -> Any:
        """Return the model.

        Returns:
            Any: An estimator object from Spark ML
        """
        return self.estimator

    @staticmethod
    def features_vector_assembler(features_cols: list[str]) -> VectorAssembler:
        """Spark transformer to assemble the feature columns into a vector.

        Args:
            features_cols (list[str]): List of feature columns to assemble

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

    def log_to_wandb(
        self: LocusToGeneModel,
        results: DataFrame,
        training_data: L2GFeatureMatrix,
        evaluators: list[
            BinaryClassificationEvaluator | MulticlassClassificationEvaluator
        ],
        wandb_run: Run,
    ) -> None:
        """Log evaluation results and feature importance to W&B.

        Args:
            results (DataFrame): Dataframe containing the predictions
            training_data (L2GFeatureMatrix): Training data used for the model. If provided, the table and the number of positive and negative labels will be logged to W&B
            evaluators (list[BinaryClassificationEvaluator | MulticlassClassificationEvaluator]): List of Spark ML evaluators to use for evaluation
            wandb_run (Run): W&B run to log the results to
        """
        ## Track evaluation metrics
        for evaluator in evaluators:
            wandb_evaluator = WandbEvaluator(
                spark_ml_evaluator=evaluator, wandb_run=wandb_run
            )
            wandb_evaluator.evaluate(results)
        ## Track feature importance
        wandb_run.log({"importances": self.get_feature_importance()})
        ## Track training set
        training_table = Table(dataframe=training_data.df.toPandas())
        wandb_run.log({"trainingSet": training_table})
        # Count number of positive and negative labels
        gs_counts_dict = {
            "goldStandard" + row["goldStandardSet"].capitalize(): row["count"]
            for row in training_data.df.groupBy("goldStandardSet").count().collect()
        }
        wandb_run.log(gs_counts_dict)
        # Missingness rates
        wandb_run.log(
            {"missingnessRates": training_data.calculate_feature_missingness_rate()}
        )

    @classmethod
    def load_from_disk(
        cls: Type[LocusToGeneModel], path: str, features_list: list[str]
    ) -> LocusToGeneModel:
        """Load a fitted pipeline model from disk.

        Args:
            path (str): Path to the model
            features_list (list[str]): List of features used for the model

        Returns:
            LocusToGeneModel: L2G model loaded from disk
        """
        return cls(model=PipelineModel.load(path), features_list=features_list)

    @classifier.setter  # type: ignore
    def classifier(self: LocusToGeneModel, new_estimator: Any) -> None:
        """Set the model.

        Args:
            new_estimator (Any): An estimator object from Spark ML
        """
        self.estimator = new_estimator

    def get_param_grid(self: LocusToGeneModel) -> list[Any]:
        """Return the parameter grid for the model.

        Returns:
            list[Any]: List of parameter maps to use for cross validation
        """
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
        hyperparameters: dict[str, Any],
        wandb_run_name: str | None,
        gold_standard_data: L2GFeatureMatrix | None = None,
    ) -> None:
        """Perform evaluation of the model predictions for the test set and track the results with W&B.

        Args:
            results (DataFrame): Dataframe containing the predictions
            hyperparameters (dict[str, Any]): Hyperparameters used for the model
            wandb_run_name (str | None): Descriptive name for the run to be tracked with W&B
            gold_standard_data (L2GFeatureMatrix | None): Feature matrix for the associations in the gold standard. If provided, the ratio of positive to negative labels will be logged to W&B
        """
        binary_evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="rawPrediction", labelCol="label"
        )
        multi_evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction"
        )

        if wandb_run_name and gold_standard_data:
            run = wandb_init(
                project=self.wandb_l2g_project_name,
                config=hyperparameters,
                name=wandb_run_name,
            )
            if isinstance(run, Run):
                self.log_to_wandb(
                    results,
                    gold_standard_data,
                    [binary_evaluator, multi_evaluator],
                    run,
                )
                run.finish()

    @property
    def feature_name_map(self: LocusToGeneModel) -> dict[str, str]:
        """Return a dictionary mapping encoded feature names to the original names.

        Returns:
            dict[str, str]: Feature name map of the model

        Raises:
            ValueError: If the model has not been fitted yet
        """
        if not self.model:
            raise ValueError("Model not fitted yet. `fit()` has to be called first.")
        elif isinstance(self.model.stages[1], VectorAssembler):
            feature_names = self.model.stages[1].getInputCols()
        return {f"f{i}": feature_name for i, feature_name in enumerate(feature_names)}

    def get_feature_importance(self: LocusToGeneModel) -> dict[str, float]:
        """Return dictionary with relative importances of every feature in the model. Feature names are encoded and have to be mapped back to their original names.

        Returns:
            dict[str, float]: Dictionary mapping feature names to their importance

        Raises:
            ValueError: If the model has not been fitted yet or is not an XGBoost model
        """
        if not self.model or not isinstance(
            self.model.stages[-1], SparkXGBClassifierModel
        ):
            raise ValueError(
                f"Model type {type(self.model)} not supported for feature importance."
            )
        importance_map = self.model.stages[-1].get_feature_importances()
        return {self.feature_name_map[k]: v for k, v in importance_map.items()}

    def fit(
        self: LocusToGeneModel,
        feature_matrix: L2GFeatureMatrix,
    ) -> LocusToGeneModel:
        """Fit the pipeline to the feature matrix dataframe.

        Args:
            feature_matrix (L2GFeatureMatrix): Feature matrix dataframe to fit the model to

        Returns:
            LocusToGeneModel: Fitted model
        """
        self.model = self.pipeline.fit(feature_matrix.df)
        return self

    def predict(
        self: LocusToGeneModel,
        feature_matrix: L2GFeatureMatrix,
    ) -> DataFrame:
        """Apply the model to a given feature matrix dataframe. The feature matrix needs to be preprocessed first.

        Args:
            feature_matrix (L2GFeatureMatrix): Feature matrix dataframe to apply the model to

        Returns:
            DataFrame: Dataframe with predictions

        Raises:
            ValueError: If the model has not been fitted yet
        """
        if not self.model:
            raise ValueError("Model not fitted yet. `fit()` has to be called first.")
        return self.model.transform(feature_matrix.df)
