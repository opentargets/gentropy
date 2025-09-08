"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from sklearn.base import clone
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from wandb.data_types import Image
from wandb.errors.term import termlog as wandb_termlog
from wandb.sdk.wandb_init import init as wandb_init
from wandb.sdk.wandb_setup import _setup
from wandb.sdk.wandb_sweep import sweep as wandb_sweep
from wandb.sklearn import plot_classifier
from wandb.wandb_agent import agent as wandb_agent

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.method.l2g.model import LocusToGeneModel

if TYPE_CHECKING:
    from matplotlib.axes._axes import Axes
    from shap._explanation import Explanation
    from wandb.sdk.wandb_run import Run
import logging


def reset_wandb_env() -> None:
    """Reset Wandb environment variables except for project, entity and API key.

    This is necessary to log multiple runs in the same sweep without overwriting. More context here: https://github.com/wandb/wandb/issues/5119
    """
    exclude = {
        "WANDB_PROJECT",
        "WANDB_ENTITY",
        "WANDB_API_KEY",
    }
    for key in list(os.environ.keys()):
        if key.startswith("WANDB_") and key not in exclude:
            del os.environ[key]


@dataclass
class LocusToGeneTrainer:
    """Modelling of what is the most likely causal gene associated with a given locus."""

    model: LocusToGeneModel
    feature_matrix: L2GFeatureMatrix

    # Initialise vars
    features_list: list[str] | None = None
    train_df: pd.DataFrame | None = None
    test_df: pd.DataFrame | None = None
    x_train: np.ndarray | None = None
    y_train: np.ndarray | None = None
    x_test: np.ndarray | None = None
    y_test: np.ndarray | None = None
    run: Run | None = None
    wandb_l2g_project_name: str = "gentropy-locus-to-gene"

    def __post_init__(self) -> None:
        """Set default features_list to feature_matrix's features_list if not provided."""
        self.features_list = (
            self.feature_matrix.features_list
            if self.features_list is None
            else self.features_list
        )

    def fit(
        self: LocusToGeneTrainer,
    ) -> LocusToGeneModel:
        """Fit the pipeline to the feature matrix dataframe.

        Returns:
            LocusToGeneModel: Fitted model

        Raises:
            ValueError: Train data not set, nothing to fit.
            AssertionError: If x_train or y_train are empty matrices
        """
        if (
            self.x_train is not None
            and self.y_train is not None
            and self.features_list is not None
        ):
            assert (
                self.x_train.size != 0 and self.y_train.size != 0
            ), "Train data not set, nothing to fit."
            fitted_model = self.model.model.fit(X=self.x_train, y=self.y_train)
            self.model = LocusToGeneModel(
                model=fitted_model,
                hyperparameters=fitted_model.get_params(),
                training_data=self.feature_matrix,
                features_list=self.features_list,
            )
            return self.model
        raise ValueError("Train data not set, nothing to fit.")

    def _get_shap_explanation(
        self: LocusToGeneTrainer,
        model: LocusToGeneModel,
    ) -> Explanation:
        """Get the SHAP values for the given model and data. We sample the full X matrix (without the labels) to interpret their shap values.

        Args:
            model (LocusToGeneModel): Model to explain.

        Returns:
                Explanation: SHAP values for the given model and data.

        Raises:
            ValueError: Train data not set, cannot get SHAP values.
            Exception: (ExplanationError) When the additivity check fails.
        """
        if self.x_train is not None and self.x_test is not None:
            training_data = pd.DataFrame(
                np.vstack((self.x_train, self.x_test)),
                columns=self.features_list,
            )
            explainer = shap.TreeExplainer(
                model.model,
                data=training_data,
                feature_perturbation="interventional",
                model_output="probability",
            )
            try:
                return explainer(training_data.sample(n=1_000))
            except Exception as e:
                if "Additivity check failed in TreeExplainer" in repr(e):
                    return explainer(
                        training_data.sample(n=1_000), check_additivity=False
                    )
                else:
                    raise

        raise ValueError("Train data not set.")

    def log_plot_image_to_wandb(
        self: LocusToGeneTrainer, title: str, plot: Axes
    ) -> None:
        """Accepts a plot object, and saves the fig to PNG to then log it in W&B.

        Args:
            title (str): Title of the plot.
            plot (Axes): Shap plot to log.

        Raises:
            ValueError: Run not set, cannot log to W&B.
        """
        if self.run is None:
            raise ValueError("Run not set, cannot log to W&B.")
        if not plot:
            # Scatter plot returns none, so we need to handle this case
            plt.savefig("tmp.png", bbox_inches="tight")
        else:
            plot.figure.savefig("tmp.png", bbox_inches="tight")
        self.run.log({title: Image("tmp.png")})
        plt.close()
        os.remove("tmp.png")

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
            RuntimeError: If dependencies are not available.
            AssertionError: If x_train or y_train are empty matrices
        """
        if (
            self.x_train is None
            or self.x_test is None
            or self.y_train is None
            or self.y_test is None
            or self.features_list is None
        ):
            raise RuntimeError("Train data not set, we cannot log to W&B.")
        assert (
            self.x_train.size != 0 and self.y_train.size != 0
        ), "Train data not set, nothing to evaluate."
        fitted_classifier = self.model.model
        y_predicted = fitted_classifier.predict(self.x_test)
        y_probas = fitted_classifier.predict_proba(self.x_test)
        self.run = wandb_init(
            project=self.wandb_l2g_project_name,
            name=wandb_run_name,
            config=fitted_classifier.get_params(),
        )
        # Track classification plots
        plot_classifier(
            self.model.model,
            self.x_train,
            self.x_test,
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
        metrics = self.evaluate(
            y_true=self.y_test, y_pred=y_predicted, y_pred_proba=y_probas
        )
        self.run.log(metrics)
        # Log feature missingness
        self.run.log(
            {
                "missingnessRates": self.feature_matrix.calculate_feature_missingness_rate()
            }
        )
        # Plot marginal contribution of each feature
        explanation = self._get_shap_explanation(self.model)
        self.log_plot_image_to_wandb(
            "Feature Contribution",
            shap.plots.bar(
                explanation, max_display=len(self.features_list), show=False
            ),
        )
        self.log_plot_image_to_wandb(
            "Beeswarm Plot",
            shap.plots.beeswarm(
                explanation, max_display=len(self.features_list), show=False
            ),
        )
        # Plot correlation between feature values and their importance
        for feature in self.features_list:
            self.log_plot_image_to_wandb(
                f"Effect of {feature} on the predictions",
                shap.plots.scatter(
                    explanation[:, feature],
                    show=False,
                ),
            )
        wandb_termlog("Logged Shapley contributions.")
        self.run.finish()

    def log_to_terminal(
        self: LocusToGeneTrainer, eval_id: str, metrics: dict[str, Any]
    ) -> None:
        """Log metrics to terminal.

        Args:
            eval_id (str): Name of the evaluation set
            metrics (dict[str, Any]): Model metrics
        """
        for metric, value in metrics.items():
            logging.info("(%s) %s: %s", eval_id, metric, value)

    def train(
        self: LocusToGeneTrainer,
        wandb_run_name: str | None = None,
        test_size: float = 0.15,
        cross_validate: bool = True,
        n_splits: int = 5,
        hyperparameter_grid: dict[str, Any] | None = None,
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        If cross_validation is set to True, we implement the following strategy:
            1. Create held-out test set
            2. Perform cross-validation on training set
            3. Train final model on full training set
            4. Evaluate once on test set

        Args:
            wandb_run_name (str | None): Name of the W&B run. Unless this is provided, the model will not be logged to W&B.
            test_size (float): Proportion of the test set
            cross_validate (bool): Whether to run cross-validation. Defaults to True.
            n_splits(int): Number of folds the data is splitted in. The model is trained and evaluated `k - 1` times. Defaults to 5.
            hyperparameter_grid (dict[str, Any] | None): Hyperparameter grid to sweep over. Defaults to None.

        Returns:
            LocusToGeneModel: Fitted model
        """
        # Create held-out test set using hierarchical splitting
        self.train_df, self.test_df = self.feature_matrix.generate_train_test_split(
            test_size=test_size,
            verbose=True,
            label_encoder=self.model.label_encoder,
            label_col=self.feature_matrix.label_col,
        )
        self.x_train = self.train_df[self.features_list].apply(pd.to_numeric).values
        self.y_train = (
            self.train_df[self.feature_matrix.label_col].apply(pd.to_numeric).values
        )
        self.x_test = self.test_df[self.features_list].apply(pd.to_numeric).values
        self.y_test = (
            self.test_df[self.feature_matrix.label_col].apply(pd.to_numeric).values
        )

        # Cross-validation
        if cross_validate:
            wandb_run_name = f"{wandb_run_name}-cv" if wandb_run_name else None
            self.cross_validate(
                wandb_run_name=wandb_run_name,
                parameter_grid=hyperparameter_grid,
                n_splits=n_splits,
            )

        # Train final model on full training set
        self.fit()

        # Evaluate once on hold out test set
        if wandb_run_name:
            wandb_run_name = f"{wandb_run_name}-holdout"
            self.log_to_wandb(wandb_run_name)
        else:
            self.log_to_terminal(
                eval_id="Hold-out",
                metrics=self.evaluate(
                    y_true=self.y_test,
                    y_pred=self.model.model.predict(self.x_test),
                    y_pred_proba=self.model.model.predict_proba(self.x_test),
                ),
            )

        return self.model

    def cross_validate(
        self: LocusToGeneTrainer,
        wandb_run_name: str | None = None,
        parameter_grid: dict[str, Any] | None = None,
        n_splits: int = 5,
        random_state: int = 42,
    ) -> None:
        """Log results of cross validation and hyperparameter tuning with W&B Sweeps. Metrics for every combination of hyperparameters will be logged to W&B for comparison.

        Args:
            wandb_run_name (str | None): Name of the W&B run. Unless this is provided, the model will not be logged to W&B.
            parameter_grid (dict[str, Any] | None): Dictionary containing the hyperparameters to sweep over. The keys are the hyperparameter names, and the values are dictionaries containing the values to sweep over.
            n_splits (int): Number of folds the data is splitted in. The model is trained and evaluated `k - 1` times. Defaults to 5.
            random_state (int): Random seed for reproducibility. Defaults to 42.
        """
        # If no grid is provided, use default ones set in the model
        parameter_grid = parameter_grid or {
            param: {"values": [value]}
            for param, value in self.model.hyperparameters.items()
        }

        def cross_validate_single_fold(
            fold_index: int,
            fold_train_df: pd.DataFrame,
            fold_val_df: pd.DataFrame,
            sweep_id: str | None,
            sweep_run_name: str | None,
            config: dict[str, Any] | None,
        ) -> None:
            """Run cross-validation for a single fold.

            Args:
                fold_index (int): Index of the fold
                fold_train_df (pd.DataFrame): Training data for the fold
                fold_val_df (pd.DataFrame): Validation data for the fold
                sweep_id (str | None): ID of the sweep, if logging to W&B is enabled
                sweep_run_name (str | None): Name of the sweep run, if logging to W&B is enabled
                config (dict[str, Any] | None): Configuration from the sweep, if logging to W&B is enabled
            """
            reset_wandb_env()

            x_fold_train, x_fold_val = (
                fold_train_df[self.features_list].values,
                fold_val_df[self.features_list].values,
            )
            y_fold_train, y_fold_val = (
                fold_train_df[self.feature_matrix.label_col].values,
                fold_val_df[self.feature_matrix.label_col].values,
            )

            fold_model = clone(self.model.model)
            fold_model.fit(x_fold_train, y_fold_train)
            y_pred_proba = fold_model.predict_proba(x_fold_val)
            y_pred = fold_model.predict(x_fold_val)

            # Log metrics
            metrics = self.evaluate(
                y_true=y_fold_val, y_pred=y_pred, y_pred_proba=y_pred_proba
            )
            if sweep_id and sweep_run_name and config:
                fold_model.set_params(**config)
                # Initialize a new run for this fold
                os.environ["WANDB_SWEEP_ID"] = sweep_id
                run = wandb_init(
                    project=self.wandb_l2g_project_name,
                    name=sweep_run_name,
                    config=config,
                    group=sweep_run_name,
                    job_type="fold",
                    reinit=True,
                )
                run.log(metrics)
                wandb_termlog(f"Logged metrics for fold {fold_index}.")
                run.finish()
            else:
                self.log_to_terminal(eval_id=f"Fold {fold_index}", metrics=metrics)

        def run_all_folds() -> None:
            """Run cross-validation for all folds."""
            # Initialise vars
            sweep_run = None
            sweep_id = None
            sweep_url = None
            sweep_group_url = None
            config = None
            if wandb_run_name:
                # Initialize the sweep run and get metadata
                sweep_run = wandb_init(name=wandb_run_name)
                sweep_id = sweep_run.sweep_id
                sweep_url = sweep_run.get_sweep_url()
                sweep_group_url = f"{sweep_run.get_project_url()}/groups/{sweep_id}"
                sweep_run.notes = sweep_group_url
                sweep_run.save()
                config = dict(sweep_run.config)

                # Reset wandb setup to ensure clean state
                _setup(_reset=True)

                wandb_termlog(f"Sweep URL: {sweep_url}")
                wandb_termlog(f"Sweep Group URL: {sweep_group_url}")

            # Split training data hierarchically for this fold and run all folds
            for fold_index in range(n_splits):
                fold_seed = random_state + fold_index
                fold_train_df, fold_val_df = LocusToGeneTrainer.hierarchical_split(
                    self.train_df,
                    verbose=False,
                    random_state=fold_seed,
                )
                cross_validate_single_fold(
                    fold_index=fold_index + 1,
                    fold_train_df=fold_train_df,
                    fold_val_df=fold_val_df,
                    sweep_id=sweep_id,
                    sweep_run_name=f"{wandb_run_name}-fold{fold_index + 1}"
                    if wandb_run_name
                    else None,
                    config=config if config else None,
                )

        if wandb_run_name:
            # Evaluate with cross validation in a W&B Sweep
            sweep_config = {
                "method": "grid",
                "name": wandb_run_name,
                "metric": {"name": "areaUnderROC", "goal": "maximize"},
                "parameters": parameter_grid,
            }
            sweep_id = wandb_sweep(sweep_config, project=self.wandb_l2g_project_name)
            wandb_agent(sweep_id, run_all_folds)
        else:
            # Evaluate with cross validation to the terminal
            run_all_folds()

    @staticmethod
    def evaluate(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_pred_proba: np.ndarray,
    ) -> dict[str, float]:
        """Evaluate the model on a test set.

        Args:
            y_true (np.ndarray): True labels
            y_pred (np.ndarray): Predicted labels
            y_pred_proba (np.ndarray): Predicted probabilities for the positive class

        Returns:
            dict[str, float]: Dictionary of evaluation metrics
        """
        return {
            "areaUnderROC": roc_auc_score(
                y_true, y_pred_proba[:, 1], average="weighted"
            ),
            "accuracy": accuracy_score(y_true, y_pred),
            "weightedPrecision": precision_score(y_true, y_pred, average="weighted"),
            "averagePrecision": average_precision_score(
                y_true, y_pred, average="weighted"
            ),
            "weightedRecall": recall_score(y_true, y_pred, average="weighted"),
            "f1": f1_score(y_true, y_pred, average="weighted"),
        }

    @staticmethod
    def hierarchical_split(
        data_df: pd.DataFrame,
        test_size: float = 0.15,
        verbose: bool = True,
        random_state: int = 777,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Implements hierarchical splitting strategy to prevent data leakage.

        Strategy:
        1. Split positives by geneId groups
        2. Further split by studyLocusId within each gene group
        3. Augment splits with corresponding negatives based on studyLocusId

        Args:
            data_df (pd.DataFrame): Input dataframe with goldStandardSet column (1=positive, 0=negative)
            test_size (float): Proportion of data for test set. Defaults to 0.15
            verbose (bool): Print splitting statistics
            random_state (int): Random seed for reproducibility. Defaults to 777

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Training and test dataframes
        """
        positives = data_df[data_df["goldStandardSet"] == 1].copy()
        negatives = data_df[data_df["goldStandardSet"] == 0].copy()

        # 1: Group positives by geneId and split genes between train/test by prioritising larger groups
        gene_groups = positives.groupby("geneId").size().reset_index(name="count")
        gene_groups = gene_groups.sort_values("count", ascending=False)

        genes_train, genes_test = train_test_split(
            gene_groups["geneId"].tolist(),
            test_size=test_size,
            shuffle=True,
            random_state=random_state,
        )

        # 2: Split by studyLocusId within each gene group
        train_study_loci = set()
        test_study_loci = set()
        train_gene_positives = positives[positives["geneId"].isin(genes_train)]
        train_study_loci.update(train_gene_positives["studyLocusId"].unique())

        test_gene_positives = positives[positives["geneId"].isin(genes_test)]
        test_study_loci.update(test_gene_positives["studyLocusId"].unique())

        # If we have overlapping loci, we assign them to train set after controlling that the overlap is not too large
        overlapping_loci = train_study_loci.intersection(test_study_loci)
        if overlapping_loci:
            test_study_loci = test_study_loci - overlapping_loci
            test_gene_positives = test_gene_positives[
                ~test_gene_positives["studyLocusId"].isin(overlapping_loci)
            ]
        if len(overlapping_loci) / len(test_study_loci) > 0.1:
            logging.warning(
                "Abundant overlap between train and test sets: %d",
                len(overlapping_loci),
            )

        # Final positive splits
        train_positives = positives[positives["studyLocusId"].isin(train_study_loci)]
        test_positives = positives[positives["studyLocusId"].isin(test_study_loci)]

        if verbose:
            logging.info("Total samples: %d", len(data_df))
            logging.info("Positives: %d", len(positives))
            logging.info("Negatives: %d", len(negatives))
            logging.info("Unique genes in positives: %d", positives["geneId"].nunique())
            logging.info(
                "Unique studyLocusIds in positives: %d",
                positives["studyLocusId"].nunique(),
            )
            logging.info("\nGene-level split:")
            logging.info("Genes in train: %d", len(genes_train))
            logging.info("Genes in test: %d", len(genes_test))
            logging.info("\nStudyLocusId-level split:")
            logging.info("StudyLocusIds in train: %d", len(train_study_loci))
            logging.info("StudyLocusIds in test: %d", len(test_study_loci))
            logging.info("Positive samples in train: %d", len(train_positives))
            logging.info("Positive samples in test: %d", len(test_positives))

        # 3: Expand splits by bringing negatives to the loci
        train_negatives = negatives[negatives["studyLocusId"].isin(train_study_loci)]
        test_negatives = negatives[negatives["studyLocusId"].isin(test_study_loci)]

        # 4: Final splits
        train_df = pd.concat([train_positives, train_negatives], ignore_index=True)
        test_df = pd.concat([test_positives, test_negatives], ignore_index=True)

        train_genes = set(train_df["geneId"].unique())
        test_genes = set(test_df["geneId"].unique())
        train_loci = set(train_df["studyLocusId"].unique())
        test_loci = set(test_df["studyLocusId"].unique())
        loci_overlap = train_loci.intersection(test_loci)
        if loci_overlap:
            logging.warning(
                "Data leakage detected! Overlapping studyLocusIds between splits."
            )
        if verbose:
            gene_overlap = train_genes.intersection(test_genes)
            logging.info("\nFinal split statistics:")
            logging.info(
                "Train set: %d samples (%d positives)",
                len(train_df),
                train_df["goldStandardSet"].sum(),
            )
            logging.info(
                "Test set: %d samples (%d positives)",
                len(test_df),
                test_df["goldStandardSet"].sum(),
            )
            logging.info(
                "Gene overlap between splits (expected): %d", len(gene_overlap)
            )
            logging.info(
                "StudyLocusId overlap between splits (not expected): %d",
                len(loci_overlap),
            )

        return train_df, test_df
