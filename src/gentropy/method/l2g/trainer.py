"""Utilities to train and apply the Locus to Gene classifier."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from sklearn.base import clone
from sklearn.metrics import (
    accuracy_score,
    auc,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
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
            assert self.x_train.size != 0 and self.y_train.size != 0, (
                "Train data not set, nothing to fit."
            )
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
        assert self.x_train.size != 0 and self.y_train.size != 0, (
            "Train data not set, nothing to evaluate."
        )
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
        train_on_full_dataset: bool = False,
        cv_results_dir: str | None = None,
    ) -> LocusToGeneModel:
        """Train the Locus to Gene model.

        The training strategy is as follows:
            1. Create held-out test set via hierarchical splitting
            2. Optionally perform cross-validation on the training set
            3. Train model on the training set (held-out set excluded)
            4. Evaluate once on the held-out test set — this is the reported benchmark
            5. Optionally retrain on the full dataset (train + held-out) for the saved model

        Step 5 follows the standard practice of using train/test splits exclusively for
        honest evaluation, then retraining on all available labelled data before saving.
        The rationale is that the held-out set gives an unbiased performance estimate, but
        withholding it from the final model needlessly discards signal — more training data
        consistently improves generalisation. The reported metrics are always from step 4
        and are not affected by whether step 5 runs.

        Args:
            wandb_run_name (str | None): Name of the W&B run. Unless this is provided, the model will not be logged to W&B.
            test_size (float): Proportion of the test set
            cross_validate (bool): Whether to run cross-validation. Defaults to True.
            n_splits(int): Number of folds the data is splitted in. The model is trained and evaluated `k - 1` times. Defaults to 5.
            hyperparameter_grid (dict[str, Any] | None): Hyperparameter grid to sweep over. Defaults to None.
            train_on_full_dataset (bool): Whether to retrain the final saved model on the full dataset (train + held-out) after evaluation. Defaults to False.
            cv_results_dir (str | None): Directory to write CV results (JSON, CSV, plots). Only used when cross_validate=True and wandb_run_name is not set. Defaults to None.

        Returns:
            LocusToGeneModel: Fitted model
        """
        # Grid search is exploratory — full-dataset retraining makes no sense when
        # no single winning config has been chosen yet.
        if train_on_full_dataset and hyperparameter_grid and any(
            len(v.get("values", [])) > 1 for v in hyperparameter_grid.values()
        ):
            logging.warning(
                "train_on_full_dataset=True is ignored during a hyperparameter grid "
                "search. Re-run with the chosen config and train_on_full_dataset=True."
            )
            train_on_full_dataset = False

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
                cv_results_dir=cv_results_dir,
            )

        # Train model on training set and evaluate on held-out test set
        self.fit()

        if wandb_run_name:
            self.log_to_wandb(f"{wandb_run_name}-holdout")
        else:
            self.log_to_terminal(
                eval_id="Hold-out",
                metrics=self.evaluate(
                    y_true=self.y_test,
                    y_pred=self.model.model.predict(self.x_test),
                    y_pred_proba=self.model.model.predict_proba(self.x_test),
                ),
            )

        # Retrain on full dataset so the saved model benefits from all labelled data.
        # Evaluation above is already complete and unaffected by this step.
        if train_on_full_dataset:
            logging.info(
                "Retraining final model on full dataset (train + held-out). "
                "Reported metrics reflect held-out performance only."
            )
            if (
                self.x_train is None
                or self.x_test is None
                or self.y_train is None
                or self.y_test is None
            ):
                raise ValueError(
                    "Training and test arrays must be initialised before retraining "
                    "on the full dataset."
                )
            self.x_train = np.vstack((self.x_train, self.x_test))
            self.y_train = np.concatenate((self.y_train, self.y_test))
            self.fit()
            if wandb_run_name:
                self.x_test = self.x_train
                self.y_test = self.y_train
                self.log_to_wandb(f"{wandb_run_name}-full-dataset")

        return self.model

    def cross_validate(
        self: LocusToGeneTrainer,
        wandb_run_name: str | None = None,
        parameter_grid: dict[str, Any] | None = None,
        n_splits: int = 5,
        random_state: int = 42,
        cv_results_dir: str | None = None,
    ) -> None:
        """Log results of cross validation and hyperparameter tuning.

        When wandb_run_name is set: runs a W&B grid sweep, one run per fold per config.
        When cv_results_dir is set (without W&B): iterates the grid explicitly and writes
        cv_results.json, cv_folds.csv, and per-config plots to cv_results_dir.
        Without either: logs fold metrics to terminal using the base hyperparameters.

        Args:
            wandb_run_name (str | None): Name of the W&B run. Unless this is provided, the model will not be logged to W&B.
            parameter_grid (dict[str, Any] | None): Dictionary containing the hyperparameters to sweep over. The keys are the hyperparameter names, and the values are dictionaries containing the values to sweep over.
            n_splits (int): Number of folds the data is splitted in. The model is trained and evaluated `k - 1` times. Defaults to 5.
            random_state (int): Random seed for reproducibility. Defaults to 42.
            cv_results_dir (str | None): Directory to write CV results. Only used when wandb_run_name is not set. Defaults to None.
        """
        # If no grid is provided, use default ones set in the model
        parameter_grid = parameter_grid or {
            param: {"values": [value]}
            for param, value in self.model.hyperparameters.items()
        }

        # Collect raw fold data for file-based output (only when not using W&B)
        collect_for_file = cv_results_dir is not None and wandb_run_name is None
        fold_results: list[dict[str, Any]] = []

        def run_all_folds(run_config: dict[str, Any] | None = None) -> None:
            """Run cross-validation for all folds with a given hyperparameter config.

            Args:
                run_config (dict[str, Any] | None): Hyperparameter config for this sweep run.
                    When called by wandb_agent, this is overridden by the sweep config.
            """
            sweep_id = None
            if wandb_run_name:
                sweep_run = wandb_init(name=wandb_run_name)
                sweep_id = sweep_run.sweep_id
                sweep_url = sweep_run.get_sweep_url()
                sweep_group_url = f"{sweep_run.get_project_url()}/groups/{sweep_id}"
                sweep_run.notes = sweep_group_url
                sweep_run.save()
                run_config = dict(sweep_run.config)
                _setup()
                wandb_termlog(f"Sweep URL: {sweep_url}")
                wandb_termlog(f"Sweep Group URL: {sweep_group_url}")

            for fold_index in range(n_splits):
                fold_seed = random_state + fold_index
                fold_train_df, fold_val_df = LocusToGeneTrainer.hierarchical_split(
                    self.train_df,
                    verbose=False,
                    random_state=fold_seed,
                )
                self._run_cv_fold(
                    fold_index=fold_index + 1,
                    fold_train_df=fold_train_df,
                    fold_val_df=fold_val_df,
                    sweep_id=sweep_id,
                    sweep_run_name=f"{wandb_run_name}-fold{fold_index + 1}"
                    if wandb_run_name
                    else None,
                    config=run_config,
                    collect_for_file=collect_for_file,
                    fold_results=fold_results,
                )

        if wandb_run_name:
            sweep_config = {
                "method": "grid",
                "name": wandb_run_name,
                "metric": {"name": "areaUnderROC", "goal": "maximize"},
                "parameters": parameter_grid,
            }
            sweep_id = wandb_sweep(sweep_config, project=self.wandb_l2g_project_name)
            wandb_agent(sweep_id, run_all_folds)
        elif cv_results_dir:
            is_gcs = cv_results_dir.startswith("gs://")
            work_dir = (
                Path(tempfile.mkdtemp(prefix="l2g_cv_"))
                if is_gcs
                else Path(cv_results_dir)
            )
            if not is_gcs:
                work_dir.mkdir(parents=True, exist_ok=True)
            config_summaries: list[dict[str, Any]] = []
            try:
                for config_id, config in enumerate(self._expand_grid(parameter_grid)):
                    fold_results.clear()
                    run_all_folds(run_config=config)
                    config_summaries.append(
                        self._summarise_and_plot_config(
                            config_id, list(fold_results), work_dir
                        )
                    )
                    fold_results.clear()  # release raw arrays before next config
                self._write_cv_files(config_summaries, work_dir, n_splits)
                if is_gcs:
                    self._upload_dir_to_gcs(work_dir, cv_results_dir)
            finally:
                if is_gcs:
                    shutil.rmtree(work_dir, ignore_errors=True)
            logging.info("CV results written to %s", cv_results_dir)
        else:
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
        cm = confusion_matrix(y_true, y_pred)
        # cm layout: [[TN, FP], [FN, TP]] for binary classification
        tn, fp, fn, tp = (int(cm[0, 0]), int(cm[0, 1]), int(cm[1, 0]), int(cm[1, 1]))
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
            "TP": tp,
            "FP": fp,
            "TN": tn,
            "FN": fn,
        }

    def _run_cv_fold(
        self: LocusToGeneTrainer,
        fold_index: int,
        fold_train_df: pd.DataFrame,
        fold_val_df: pd.DataFrame,
        sweep_id: str | None,
        sweep_run_name: str | None,
        config: dict[str, Any] | None,
        collect_for_file: bool,
        fold_results: list[dict[str, Any]],
    ) -> None:
        """Train and evaluate the model on a single cross-validation fold.

        Args:
            fold_index (int): 1-based fold index used for logging.
            fold_train_df (pd.DataFrame): Training data for this fold.
            fold_val_df (pd.DataFrame): Validation data for this fold.
            sweep_id (str | None): W&B sweep ID; None when not using W&B.
            sweep_run_name (str | None): W&B run name for this fold; None when not using W&B.
            config (dict[str, Any] | None): Hyperparameter config to apply before fitting.
            collect_for_file (bool): Whether to append fold data to fold_results.
            fold_results (list[dict[str, Any]]): Mutable list that fold data is appended to.
        """
        reset_wandb_env()

        x_fold_train = fold_train_df[self.features_list].values
        x_fold_val = fold_val_df[self.features_list].values
        y_fold_train = fold_train_df[self.feature_matrix.label_col].values
        y_fold_val = fold_val_df[self.feature_matrix.label_col].values

        fold_model = clone(self.model.model)
        if config:
            fold_model.set_params(**config)
        fold_model.fit(x_fold_train, y_fold_train)
        y_pred_proba = fold_model.predict_proba(x_fold_val)
        y_pred = fold_model.predict(x_fold_val)

        metrics = self.evaluate(
            y_true=y_fold_val, y_pred=y_pred, y_pred_proba=y_pred_proba
        )

        # Locus-level resolution stats
        locus_proba = pd.DataFrame(
            {"studyLocusId": fold_val_df["studyLocusId"].values, "prob": y_pred_proba[:, 1]}
        )
        genes_above = locus_proba[locus_proba["prob"] >= 0.5].groupby("studyLocusId").size()
        all_loci = locus_proba["studyLocusId"].unique()
        metrics["n_loci"] = int(len(all_loci))
        metrics["n_loci_one_gene_above"] = int((genes_above == 1).sum())
        metrics["n_loci_no_gene_above"] = int(
            len(all_loci) - len(genes_above)
        )

        if collect_for_file:
            fold_results.append(
                {
                    "config": dict(config) if config else {},
                    "fold": fold_index,
                    "metrics": metrics,
                    "y_true": y_fold_val,
                    "y_pred_proba": y_pred_proba[:, 1],
                }
            )

        if sweep_id and sweep_run_name and config:
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

    @staticmethod
    def _expand_grid(parameter_grid: dict[str, Any]) -> list[dict[str, Any]]:
        """Expand a W&B-style parameter grid into a flat list of hyperparameter configs.

        Args:
            parameter_grid (dict[str, Any]): Grid in the form {"param": {"values": [v1, v2, ...]}, ...}

        Returns:
            list[dict[str, Any]]: One dict per hyperparameter combination.
        """
        keys = list(parameter_grid.keys())
        missing = [k for k in keys if "values" not in parameter_grid[k]]
        if missing:
            raise ValueError(
                f"Hyperparameter grid entries must have a 'values' key. "
                f"Missing in: {missing}"
            )
        values = [parameter_grid[k]["values"] for k in keys]
        return [dict(zip(keys, combo)) for combo in product(*values)]

    def _summarise_and_plot_config(
        self: LocusToGeneTrainer,
        config_id: int,
        folds: list[dict[str, Any]],
        work_dir: Path,
    ) -> dict[str, Any]:
        """Write per-config plots and return a summary dict without raw arrays.

        Called once per hyperparameter config immediately after its folds complete,
        so raw y_true/y_pred_proba arrays can be released before the next config runs.

        Args:
            config_id (int): Zero-based index of this config in the sweep.
            folds (list[dict[str, Any]]): Fold data dicts (metrics, y_true, y_pred_proba).
            work_dir (Path): Root output directory; config_{config_id}/ is created here.

        Returns:
            dict[str, Any]: Summary with hyperparameters, per-fold metrics, mean/std — no raw arrays.
        """
        metric_keys = list(folds[0]["metrics"].keys())
        mean_metrics = {
            k: float(np.mean([f["metrics"][k] for f in folds])) for k in metric_keys
        }
        std_metrics = {
            k: float(np.std([f["metrics"][k] for f in folds])) for k in metric_keys
        }

        config_dir = work_dir / f"config_{config_id}"
        config_dir.mkdir(exist_ok=True)
        self._plot_roc_curves(folds, config_dir / "roc.png")
        self._plot_pr_curves(folds, config_dir / "pr.png")
        self._plot_confusion_matrix(folds, config_dir / "confusion_matrix.png")

        return {
            "config_id": config_id,
            "hyperparameters": folds[0]["config"],
            "fold_metrics": [
                {
                    "fold": f["fold"],
                    **{k: int(v) if isinstance(v, (int, np.integer)) else float(v)
                       for k, v in f["metrics"].items()},
                }
                for f in folds
            ],
            "mean_metrics": mean_metrics,
            "std_metrics": std_metrics,
        }

    @staticmethod
    def _write_cv_files(
        config_summaries: list[dict[str, Any]],
        work_dir: Path,
        n_splits: int,
    ) -> None:
        """Write cv_results.json and cv_folds.csv from pre-computed config summaries.

        Args:
            config_summaries (list[dict[str, Any]]): One summary per config from _summarise_and_plot_config.
            work_dir (Path): Directory to write files into.
            n_splits (int): Number of folds used.
        """
        output: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "n_splits": n_splits,
            "n_configs": len(config_summaries),
            "configs": config_summaries,
        }
        with open(work_dir / "cv_results.json", "w") as fh:
            json.dump(output, fh, indent=2)

        rows = []
        for cfg in config_summaries:
            for fold in cfg["fold_metrics"]:
                rows.append(
                    {"config_id": cfg["config_id"], **cfg["hyperparameters"], **fold}
                )
        pd.DataFrame(rows).to_csv(work_dir / "cv_folds.csv", index=False)

    @staticmethod
    def _upload_dir_to_gcs(local_dir: Path, gcs_prefix: str) -> None:
        """Upload every file under local_dir to GCS, preserving relative paths.

        Args:
            local_dir (Path): Root of the local directory tree to upload.
            gcs_prefix (str): Destination GCS prefix, e.g. gs://bucket/path.
        """
        from google.cloud import storage as gcs_storage

        without_scheme = gcs_prefix[len("gs://"):]
        bucket_name, _, blob_prefix = without_scheme.partition("/")
        client = gcs_storage.Client()
        bucket = client.bucket(bucket_name)

        for local_file in local_dir.rglob("*"):
            if not local_file.is_file():
                continue
            relative = local_file.relative_to(local_dir)
            blob_name = f"{blob_prefix.rstrip('/')}/{relative}" if blob_prefix else str(relative)
            bucket.blob(blob_name).upload_from_filename(str(local_file))
            logging.info("Uploaded %s → gs://%s/%s", relative, bucket_name, blob_name)

    def _plot_roc_curves(
        self: LocusToGeneTrainer,
        folds: list[dict[str, Any]],
        output_path: Path,
    ) -> None:
        """Plot per-fold ROC curves with mean AUC in the title.

        Args:
            folds (list[dict[str, Any]]): Fold data dicts with y_true and y_pred_proba keys.
            output_path (Path): Where to save the PNG.
        """
        fig, ax = plt.subplots(figsize=(7, 6))
        fold_aucs = []
        for fold in folds:
            fpr, tpr, _ = roc_curve(fold["y_true"], fold["y_pred_proba"])
            fold_auc = auc(fpr, tpr)
            fold_aucs.append(fold_auc)
            ax.plot(fpr, tpr, alpha=0.5, lw=1.2, label=f"fold {fold['fold']} (AUC={fold_auc:.3f})")
        ax.plot([0, 1], [0, 1], "k--", lw=0.8)
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("True Positive Rate")
        ax.set_title(
            f"ROC curves — mean AUC = {np.mean(fold_aucs):.3f} ± {np.std(fold_aucs):.3f}"
        )
        ax.legend(fontsize=8, loc="lower right")
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()

    def _plot_pr_curves(
        self: LocusToGeneTrainer,
        folds: list[dict[str, Any]],
        output_path: Path,
    ) -> None:
        """Plot per-fold Precision-Recall curves with mean AP in the title.

        Args:
            folds (list[dict[str, Any]]): Fold data dicts with y_true and y_pred_proba keys.
            output_path (Path): Where to save the PNG.
        """
        fig, ax = plt.subplots(figsize=(7, 6))
        fold_aps = []
        for fold in folds:
            precision, recall, _ = precision_recall_curve(
                fold["y_true"], fold["y_pred_proba"]
            )
            ap = average_precision_score(fold["y_true"], fold["y_pred_proba"])
            fold_aps.append(ap)
            ax.plot(recall, precision, alpha=0.5, lw=1.2, label=f"fold {fold['fold']} (AP={ap:.3f})")
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.set_title(
            f"Precision-Recall curves — mean AP = {np.mean(fold_aps):.3f} ± {np.std(fold_aps):.3f}"
        )
        ax.legend(fontsize=8, loc="upper right")
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()

    def _plot_confusion_matrix(
        self: LocusToGeneTrainer,
        folds: list[dict[str, Any]],
        output_path: Path,
    ) -> None:
        """Plot confusion matrix aggregated across all folds at threshold 0.5.

        Args:
            folds (list[dict[str, Any]]): Fold data dicts with y_true and y_pred_proba keys.
            output_path (Path): Where to save the PNG.
        """
        y_true_all = np.concatenate([f["y_true"] for f in folds])
        y_pred_all = (
            np.concatenate([f["y_pred_proba"] for f in folds]) >= 0.5
        ).astype(int)
        cm = confusion_matrix(y_true_all, y_pred_all)
        classes = list(self.model.label_encoder.values())

        fig, ax = plt.subplots(figsize=(5, 4))
        im = ax.imshow(cm, cmap="Blues")
        plt.colorbar(im, ax=ax)
        ax.set_xticks(range(len(classes)))
        ax.set_yticks(range(len(classes)))
        ax.set_xticklabels(classes)
        ax.set_yticklabels(classes)
        ax.set_xlabel("Predicted label")
        ax.set_ylabel("True label")
        ax.set_title("Confusion matrix  (threshold = 0.5, all folds)")
        thresh = cm.max() / 2.0
        for i in range(len(classes)):
            for j in range(len(classes)):
                ax.text(
                    j,
                    i,
                    str(cm[i, j]),
                    ha="center",
                    va="center",
                    color="white" if cm[i, j] > thresh else "black",
                )
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()

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
