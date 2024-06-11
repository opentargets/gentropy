"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

import wandb
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.model import LocusToGeneModel
from wandb.data_types import Table


@dataclass
class LocusToGeneTrainer:
    """Modelling of what is the most likely causal gene associated with a given locus."""

    model: LocusToGeneModel
    feature_matrix: L2GFeatureMatrix

    # Initialise vars
    features_list: list[str] | None = None
    target_labels: list[str] | None = None
    x_train: pd.DataFrame | None = None
    y_train: pd.Series | None = None
    x_test: pd.DataFrame | None = None
    y_test: pd.Series | None = None
    wandb_l2g_project_name: str = "gentropy-locus-to-gene"

    def fit(
        self: LocusToGeneTrainer,
    ) -> LocusToGeneModel:
        """Fit the pipeline to the feature matrix dataframe.

        Returns:
            LocusToGeneModel: Fitted model

        Raises:
            ValueError: Train data not set, nothing to fit.
        """
        if self.x_train is not None and self.y_train is not None:
            assert (
                not self.x_train.empty and not self.y_train.empty
            ), "Train data not set, nothing to fit."
            fitted_model = self.model.model.fit(X=self.x_train.values, y=self.y_train)
            self.model = LocusToGeneModel(
                model=fitted_model,
                hyperparameters=fitted_model.get_params(),
                training_data=self.feature_matrix,
            )
            return self.model
        raise ValueError("Train data not set, nothing to fit.")

    def log_to_wandb(
        self: LocusToGeneTrainer,
        wandb_run_name: str,
    ) -> None:
        """Log evaluation results and feature importance to W&B to compare between different L2G runs.

        Dashboard is available at https://wandb.ai/open-targets/gentropy-locus-to-gene?nw=nwuseropentargets
        Credentials to access W&B are available at the OT central login sheet.

        Args:
            wandb_run_name (str): Name of the W&B run

        Raises:
            ValueError: Train data not set, nothing to evaluate.
        """
        if (
            self.x_train is not None
            and self.x_test is not None
            and self.y_train is not None
            and self.y_test is not None
        ):
            assert (
                not self.x_train.empty and not self.y_train.empty
            ), "Train data not set, nothing to evaluate."
            fitted_classifier = self.model.model
            y_predicted = fitted_classifier.predict(self.x_test.values)
            y_probas = fitted_classifier.predict_proba(self.x_test.values)
            with wandb.init(  # type: ignore
                project=self.wandb_l2g_project_name,
                name=wandb_run_name,
                config=fitted_classifier.get_params(),
            ) as run:
                # Track classification plots
                wandb.sklearn.plot_classifier(
                    self.model.model,
                    self.x_train.values,
                    self.x_test.values,
                    self.y_train,
                    self.y_test,
                    y_predicted,
                    y_probas,
                    labels=list(self.model.label_encoder.values()),
                    model_name="L2G-classifier",
                    feature_names=self.features_list,
                    is_binary=True,
                )
                # Track evaluation metrics
                run.log(
                    {
                        "areaUnderROC": roc_auc_score(
                            self.y_test, y_probas[:, 1], average="weighted"
                        )
                    }
                )
                run.log({"accuracy": accuracy_score(self.y_test, y_predicted)})
                run.log(
                    {
                        "weightedPrecision": precision_score(
                            self.y_test, y_predicted, average="weighted"
                        )
                    }
                )
                run.log(
                    {
                        "weightedRecall": recall_score(
                            self.y_test, y_predicted, average="weighted"
                        )
                    }
                )
                run.log({"f1": f1_score(self.y_test, y_predicted, average="weighted")})
                # Track gold standards and their features
                run.log(
                    {
                        "featureMatrix": Table(
                            dataframe=self.feature_matrix.df.toPandas()
                        )
                    }
                )
                # Log feature missingness
                run.log(
                    {
                        "missingnessRates": self.feature_matrix.calculate_feature_missingness_rate()
                    }
                )
        else:
            raise ValueError("Train data not set, nothing to evaluate.")

    def train(
        self: LocusToGeneTrainer,
        wandb_run_name: str,
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        Args:
            wandb_run_name (str): Name of the W&B run. Unless this is provided, the model will not be logged to W&B.

        Returns:
            LocusToGeneModel: Fitted model
        """
        data_df = self.feature_matrix.df.drop("geneId").toPandas()

        # Encode labels in `goldStandardSet` to a numeric value
        data_df["goldStandardSet"] = data_df["goldStandardSet"].map(  # type: ignore
            self.model.label_encoder
        )

        # Convert all columns to numeric and split
        data_df = data_df.apply(pd.to_numeric)  # type: ignore
        self.feature_cols = [
            col
            for col in data_df.columns  # type: ignore
            if col not in ["studyLocusId", "goldStandardSet"]
        ]
        label_col = "goldStandardSet"
        X = data_df[self.feature_cols].copy()  # type: ignore
        y = data_df[label_col].copy()  # type: ignore
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Train
        model = self.fit()

        # Evaluate
        self.log_to_wandb(
            wandb_run_name=wandb_run_name,
        )

        return model

    # def hyperparameter_tuning(
    #     self: LocusToGeneTrainer, parameter_grid: dict
    # ) -> LocusToGeneModel:
    #     """Perform hyperparameter tuning on the model with W&B Sweeps, and return the best model.

    #     Returns:
    #         LocusToGeneModel: Fitted model with the best hyperparameters
    #     """
    #     sweep_config = {
    #         "method": "grid",
    #         "metric": {"name": "roc", "goal": "maximize"},
    #     }
    #     sweep_config["parameters"] = parameter_grid
    #     sweep_id = wandb.sweep(sweep_config, project=self.wandb_l2g_project_name)

    #     wandb.agent(sweep_id, function=self.train)
    #     best_params = sweep_id.best_run()

    # @classmethod
    # def cross_validate(
    #     cls: type[LocusToGeneTrainer],
    #     l2g_model: LocusToGeneModel,
    #     data: L2GFeatureMatrix,
    #     num_folds: int,
    #     param_grid: Optional[list] = None,  # type: ignore
    # ) -> LocusToGeneModel:
    #     """Perform k-fold cross validation on the model.

    #     By providing a model with a parameter grid, this method will perform k-fold cross validation on the model for each
    #     combination of parameters and return the best model.

    #     Args:
    #         l2g_model (LocusToGeneModel): Model to fit to the data on
    #         data (L2GFeatureMatrix): Data to perform cross validation on
    #         num_folds (int): Number of folds to use for cross validation
    #         param_grid (Optional[list]): List of parameter maps to use for cross validation

    #     Returns:
    #         LocusToGeneModel: Trained model fitted with the best hyperparameters

    #     Raises:
    #         ValueError: Parameter grid is empty. Cannot perform cross-validation.
    #         ValueError: Unable to retrieve the best model.
    #     """
    #     evaluator = MulticlassClassificationEvaluator()
    #     params_grid = param_grid or l2g_model.get_param_grid()
    #     if not param_grid:
    #         raise ValueError(
    #             "Parameter grid is empty. Cannot perform cross-validation."
    #         )
    #     cv = CrossValidator(
    #         numFolds=num_folds,
    #         estimator=l2g_model.estimator,
    #         estimatorParamMaps=params_grid,
    #         evaluator=evaluator,
    #         parallelism=2,
    #         collectSubModels=False,
    #         seed=42,
    #     )

    #     l2g_model.add_pipeline_stage(cv)  # type: ignore[assignment, unused-ignore]

    #     # Integrate the best model from the last stage of the pipeline
    #     if (full_pipeline_model := l2g_model.fit(data).model) is None or not hasattr(
    #         full_pipeline_model, "stages"
    #     ):
    #         raise ValueError("Unable to retrieve the best model.")
    #     l2g_model.model = full_pipeline_model.stages[-1].bestModel  # type: ignore[assignment, unused-ignore]
    #     return l2g_model
    #         full_pipeline_model, "stages"
    #     ):
    #         raise ValueError("Unable to retrieve the best model.")
    #     l2g_model.model = full_pipeline_model.stages[-1].bestModel  # type: ignore[assignment, unused-ignore]
    #     return l2g_model
    #     return l2g_model
