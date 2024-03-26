"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.model import LocusToGeneModel


@dataclass
class LocusToGeneTrainer:
    """Modelling of what is the most likely causal gene associated with a given locus."""

    _model: LocusToGeneModel
    train_set: L2GFeatureMatrix

    @classmethod
    def train(
        cls: type[LocusToGeneTrainer],
        gold_standard_data: L2GFeatureMatrix,
        l2g_model: LocusToGeneModel,
        evaluate: bool,
        wandb_run_name: str | None = None,
        model_path: str | None = None,
        **hyperparams: dict[str, Any],
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        Args:
            gold_standard_data (L2GFeatureMatrix): Feature matrix for the associations in the gold standard
            l2g_model (LocusToGeneModel): Model to fit to the data on
            evaluate (bool): Whether to evaluate the model on a test set
            wandb_run_name (str | None): Descriptive name for the run to be tracked with W&B
            model_path (str | None): Path to save the model to
            **hyperparams (dict[str, Any]): Hyperparameters to use for the model

        Returns:
            LocusToGeneModel: Trained model
        """
        train, test = gold_standard_data.train_test_split(fraction=0.8)

        model = l2g_model.add_pipeline_stage(l2g_model.estimator).fit(train)

        if evaluate:
            l2g_model.evaluate(
                results=model.predict(test),
                hyperparameters=hyperparams,
                wandb_run_name=wandb_run_name,
                gold_standard_data=gold_standard_data,
            )
        if model_path:
            l2g_model.save(model_path)
        return l2g_model

    @classmethod
    def cross_validate(
        cls: type[LocusToGeneTrainer],
        l2g_model: LocusToGeneModel,
        data: L2GFeatureMatrix,
        num_folds: int,
        param_grid: Optional[list] = None,  # type: ignore
    ) -> LocusToGeneModel:
        """Perform k-fold cross validation on the model.

        By providing a model with a parameter grid, this method will perform k-fold cross validation on the model for each
        combination of parameters and return the best model.

        Args:
            l2g_model (LocusToGeneModel): Model to fit to the data on
            data (L2GFeatureMatrix): Data to perform cross validation on
            num_folds (int): Number of folds to use for cross validation
            param_grid (Optional[list]): List of parameter maps to use for cross validation

        Returns:
            LocusToGeneModel: Trained model fitted with the best hyperparameters

        Raises:
            ValueError: Parameter grid is empty. Cannot perform cross-validation.
            ValueError: Unable to retrieve the best model.
        """
        evaluator = MulticlassClassificationEvaluator()
        params_grid = param_grid or l2g_model.get_param_grid()
        if not param_grid:
            raise ValueError(
                "Parameter grid is empty. Cannot perform cross-validation."
            )
        cv = CrossValidator(
            numFolds=num_folds,
            estimator=l2g_model.estimator,
            estimatorParamMaps=params_grid,
            evaluator=evaluator,
            parallelism=2,
            collectSubModels=False,
            seed=42,
        )

        l2g_model.add_pipeline_stage(cv)  # type: ignore[assignment, unused-ignore]

        # Integrate the best model from the last stage of the pipeline
        if (full_pipeline_model := l2g_model.fit(data).model) is None or not hasattr(
            full_pipeline_model, "stages"
        ):
            raise ValueError("Unable to retrieve the best model.")
        l2g_model.model = full_pipeline_model.stages[-1].bestModel  # type: ignore[assignment, unused-ignore]
        return l2g_model
