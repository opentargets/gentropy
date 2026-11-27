"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from wandb.sdk.wandb_login import login as wandb_login
from xgboost import XGBClassifier

from gentropy.common.session import Session
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.pathway_enrichment import PathwayEnrichment
from gentropy.dataset.pathway_index import PathwayIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.external.hf_hub import (
    HuggingFaceHubCredentials,
    HuggingFaceModelRepoHandle,
)
from gentropy.external.wandb import WandbCredentials
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader
from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.method.l2g.trainer import LocusToGeneTrainer

logger = logging.getLogger(__name__)


class LocusToGeneFeatureMatrixStep:
    """Annotate credible set with functional genomics features."""

    def __init__(
        self,
        session: Session,
        *,
        features_list: list[str],
        credible_set_path: str,
        credible_set_variant_path: str,
        variant_index_path: str | None = None,
        colocalisation_path: str | None = None,
        study_index_path: str | None = None,
        target_index_path: str | None = None,
        intervals_path: str | None = None,
        gene_interactions_path: str | None = None,
        pathway_index_path: str | None = None,
        pathway_enrichment_path: str | None = None,
        feature_matrix_path: str,
        append_null_features: bool = False,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            features_list (list[str]): List of features to use for the model
            credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
            credible_set_variant_path (str): Path to the variants of the credible sets
            variant_index_path (str | None): Path to the variant index dataset
            colocalisation_path (str | None): Path to the colocalisation dataset
            study_index_path (str | None): Path to the study index dataset
            target_index_path (str | None): Path to the target index dataset
            intervals_path (str | None): Path to the interval dataset
            gene_interactions_path (str | None): Path to the protein-protein interaction (PPI) dataset
            pathway_index_path (str | None): Path to the harmonised `PathwayIndex` dataset, as
                written by the pathway ingestion step
            pathway_enrichment_path (str | None): Path to the harmonised `PathwayEnrichment`
                dataset, as written by the pathway ingestion step
            feature_matrix_path (str): Path to the L2G feature matrix output dataset
            append_null_features (bool): Whether to append null features to the feature matrix. Defaults to False.
        """
        credible_set = CredibleSet.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        ).to_locus_view(
            CredibleSetVariant.from_parquet(
                session, credible_set_variant_path, recursiveFileLookup=True
            )
        )
        studies = (
            StudyIndex.from_parquet(session, study_index_path, recursiveFileLookup=True)
            if study_index_path
            else None
        )
        variant_index = (
            VariantIndex.from_parquet(session, variant_index_path)
            if variant_index_path
            else None
        )
        coloc = (
            Colocalisation.from_parquet(
                session, colocalisation_path, recursiveFileLookup=True
            )
            if colocalisation_path
            else None
        )

        target_index = (
            TargetIndex.from_parquet(
                session, target_index_path, recursiveFileLookup=True
            )
            if target_index_path
            else None
        )

        intervals = (
            Intervals.from_parquet(session, intervals_path, recursiveFileLookup=True)
            if intervals_path
            else None
        )

        interactions = (
            session.load_data(
                gene_interactions_path, "parquet", recursiveFileLookup=True
            )
            if gene_interactions_path
            else None
        )

        pathway_index = (
            PathwayIndex.from_parquet(session, pathway_index_path)
            if pathway_index_path
            else None
        )

        pathway_enrichment = (
            PathwayEnrichment.from_parquet(session, pathway_enrichment_path)
            if pathway_enrichment_path
            else None
        )

        trans_pqtl_features = {
            "transPQtlColocH4Maximum",
            "transPQtlColocH4MaximumNeighbourhood",
        }
        if trans_pqtl_features.intersection(features_list) and interactions is None:
            raise ValueError(
                "Interactions are required for trans-pQTL colocalisation features. "
                "Provide `gene_interactions_path`."
            )
        if trans_pqtl_features.intersection(features_list) and target_index is None:
            raise ValueError(
                "target_index is required for trans-pQTL colocalisation features. "
                "Provide `target_index_path`."
            )

        pathway_features = {
            "pathwayEnrichment500kb",
            "pathwayEnrichment500kbNeighbourhood",
        }
        if pathway_features.intersection(features_list) and (
            pathway_index is None
            or pathway_enrichment is None
            or target_index is None
            or studies is None
        ):
            raise ValueError(
                "Pathway enrichment features need the pathway index, the enrichment "
                "results, the target index and the study index. Provide "
                "`pathway_index_path`, `pathway_enrichment_path`, `target_index_path` "
                "and `study_index_path`."
            )

        features_input_loader = L2GFeatureInputLoader(
            variant_index=variant_index,
            colocalisation=coloc,
            study_index=studies,
            study_locus=credible_set,
            target_index=target_index,
            intervals=intervals,
            interactions=interactions,
            pathway_index=pathway_index,
            pathway_enrichment=pathway_enrichment,
        )

        fm = credible_set.filter(f.col("studyType") == "gwas").build_feature_matrix(
            features_list,
            features_input_loader,
            append_null_features=append_null_features,
        )

        if target_index is not None:
            target_index_df = target_index.df.select("id", "biotype").withColumnRenamed(
                "id", "geneId"
            )

            target_index_df = target_index_df.withColumn(
                "isProteinCoding",
                f.when(f.col("biotype") == "protein_coding", 1).otherwise(0),
            ).drop("biotype")

            fm._df = fm._df.drop("isProteinCoding").join(
                target_index_df, on="geneId", how="inner"
            )

        fm._df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(feature_matrix_path)


class LocusToGeneStep:
    """Locus to gene step."""

    def __init__(
        self,
        session: Session,
        *,
        run_mode: str,
        hyperparameters: dict[str, Any],
        download_from_hub: bool,
        train_on_full_dataset: bool,
        credible_set_path: str | None = None,
        feature_matrix_path: str | None = None,
        wandb_run_name: str | None = None,
        model_path: str = "opentargets/locus_to_gene",
        features_list: list[str] | None = None,
        predictions_path: str | None = None,
        l2g_threshold: float = 0.05,
        hf_hub_repo_id: str = "locus_to_gene",
        hf_model_commit_message: str = "chore: update model",
        hf_model_version: str | None = None,
        explain_predictions: bool = False,
        hf_credentials_path: str | None = None,
        wandb_credentials_path: str | None = None,
        train_parquet_path: str | None = None,
        test_parquet_path: str | None = None,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            run_mode (str): Run mode, either 'train' or 'predict'
            hyperparameters (dict[str, Any]): Hyperparameters for the model
            download_from_hub (bool): Whether to download the model from Hugging Face Hub
            train_on_full_dataset (bool): Whether to retrain the final saved model on the full dataset (train + held-out) after evaluation. Follows the standard practice of reporting honest held-out metrics while ensuring the deployed model benefits from all available labelled data.
            credible_set_path (str | None): Path to the credible set dataset. Required for predict mode; unused in train mode when pre-split parquets are provided.
            feature_matrix_path (str | None): Path to the L2G feature matrix input dataset. Required for predict mode; unused in train mode when pre-split parquets are provided.
            wandb_run_name (str | None): Name of the run to track model training in Weights and Biases
            model_path (str): Path to the model. It can be either in the filesystem or the name on the Hugging Face Hub (in the form of username/repo_name).
            features_list (list[str] | None): List of features to use to train the model
            predictions_path (str | None): Path to the L2G predictions output dataset
            l2g_threshold (float): An optional threshold for the L2G score to filter predictions. A threshold of 0.05 is recommended.
            hf_hub_repo_id (str): Hugging Face Hub repository handle in ``username/repo_name`` format. Used to download the model when ``download_from_hub`` is ``True`` (predict mode) and to upload the trained model (train mode).
            hf_model_commit_message (str): Commit message when we upload the model to the Hugging Face Hub
            hf_model_version (str | None): Tag, branch, or commit hash to download the model from the Hub. Defaults to latest commit when provided None.
            explain_predictions (bool): Whether to extract SHAP importances for the L2G predictions. This is computationally expensive.
            hf_credentials_path (str | None): Optional path to the Hugging Face Hub credentials JSON file. If not provided, the HF_TOKEN environment variable will be used.
            wandb_credentials_path (str | None): Optional path to the Weights and Biases credentials JSON file. If not provided, the WANDB_API_KEY environment variable will be used.
            train_parquet_path (str | None): Path to the training split parquet: feature matrix rows with ``goldStandardSet`` encoded as 0/1. Required in train mode.
            test_parquet_path (str | None): Path to the test split parquet, in the same layout as ``train_parquet_path``. Required in train mode.

        Raises:
            ValueError: If run_mode is not 'train' or 'predict'


        Note: One can fetch the credentials for HF Hub and W&B from environment variables or from JSON files. The JSON file for HF Hub should contain a
            field "HF_TOKEN" with the Hugging Face API token as value, while the JSON file for W&B should contain a field "WANDB_API_KEY" with the W&B API key as value.


        #### Credential files

        Example of credentials JSON file content for Hugging Face Hub:

        ```json
        {
            "HF_TOKEN": "your_hugging_face_api_token"
        }
        ```

        Example of credentials JSON file content for W&B:

        ```json
        {
            "WANDB_API_KEY": "your_wandb_api_key"
        }
        ```
        """
        if run_mode not in ["train", "predict"]:
            raise ValueError(
                f"run_mode must be one of 'train' or 'predict', got {run_mode}"
            )
        # Common parameters
        self.session = session
        self.run_mode = run_mode
        self.features_list = list(features_list) if features_list else None

        # Train io
        self.train_parquet_path = train_parquet_path
        self.test_parquet_path = test_parquet_path
        self.train_on_full_dataset = train_on_full_dataset

        # Train parameters
        self.hyperparameters = dict(hyperparameters)

        # External resource parameters
        self.hf_hub_repo_id = hf_hub_repo_id
        self.hf_model_commit_message = hf_model_commit_message
        self.hf_model_version = hf_model_version
        self.hf_credentials_path = hf_credentials_path
        self.wandb_run_name = wandb_run_name
        self.wandb_credentials_path = wandb_credentials_path

        # Predict io
        self.download_from_hub = download_from_hub
        self.model_path = model_path
        self.predictions_path = predictions_path

        # Predict parameters
        self.l2g_threshold = l2g_threshold
        self.explain_predictions = explain_predictions

        if run_mode == "predict":
            if not credible_set_path:
                raise ValueError("credible_set_path is required for predict mode.")
            if not feature_matrix_path:
                raise ValueError("feature_matrix_path is required for predict mode.")
            self.credible_set = CredibleSet.from_parquet(
                session, credible_set_path, recursiveFileLookup=True
            )
            self.feature_matrix = L2GFeatureMatrix(
                _df=session.load_data(feature_matrix_path, "parquet"),
            )
            if download_from_hub:
                if not self.hf_hub_repo_id:
                    raise ValueError(
                        "hf_hub_repo_id must be provided when download_from_hub is True"
                    )
                self.model_path = HuggingFaceModelRepoHandle(
                    handle=self.hf_hub_repo_id
                ).handle
            self.run_predict()
        elif run_mode == "train":
            self.run_train()

    def run_predict(self) -> None:
        """Run the prediction step.

        Raises:
            ValueError: If predictions_path is not provided for prediction mode
        """
        hf_token = None
        if self.download_from_hub:
            hf_hub_credentials = HuggingFaceHubCredentials.read(
                self.hf_credentials_path
            )
            hf_token = hf_hub_credentials.HF_TOKEN

        if not self.predictions_path:
            raise ValueError("predictions_path must be provided for prediction mode")
        predictions = (
            L2GPrediction.from_credible_set(
                self.session,
                self.credible_set,
                self.feature_matrix,
                model_path=self.model_path,
                features_list=self.features_list,
                hf_token=hf_token,
                hf_model_version=self.hf_model_version,
                download_from_hub=self.download_from_hub,
            )
            .filter(f.col("score") >= self.l2g_threshold)
            .add_features(
                self.feature_matrix,
            )
        )
        if self.explain_predictions:
            predictions = predictions.explain()
        predictions.df.coalesce(self.session.output_partitions).write.mode(
            self.session.write_mode
        ).parquet(self.predictions_path)
        self.session.logger.info("L2G predictions saved successfully.")

    def run_train(self) -> None:
        """Run the training step.

        Raises:
            ValueError: If features list or presplit parquet paths are not provided.
        """
        if self.features_list is None:
            raise ValueError("Features list is required for model training.")
        if not (self.train_parquet_path and self.test_parquet_path):
            raise ValueError(
                "train_parquet_path and test_parquet_path are required for model training."
            )

        # Initialize access to weights and biases
        if self.wandb_run_name:
            wandb_credentials = WandbCredentials.read(self.wandb_credentials_path)
            wandb_login(key=wandb_credentials.WANDB_API_KEY.get_secret_value())

        # Initialize access to Hugging Face Hub
        hf_token = None
        if self.hf_hub_repo_id and self.hf_model_commit_message:
            hf_hub_credentials = HuggingFaceHubCredentials.read(
                self.hf_credentials_path
            )
            hf_token = hf_hub_credentials.HF_TOKEN

        # Instantiate classifier and train model
        l2g_model = LocusToGeneModel(
            model=XGBClassifier(random_state=777, eval_metric="aucpr"),
            hyperparameters=self.hyperparameters,
            features_list=self.features_list,
        )

        # Load the presplit train and test data.
        # Persist so toPandas() (for XGBoost) and the union (for HF Hub metadata) share the cache.
        train_sdf = self.session.spark.read.parquet(self.train_parquet_path).persist()
        test_sdf = self.session.spark.read.parquet(self.test_parquet_path).persist()

        # Reconstruct L2GFeatureMatrix for model metadata (column order, HF Hub upload).
        # Labels are decoded back to strings because export_to_hugging_face_hub calls
        # generate_train_test_split, which expects string-encoded labels.
        label_map = f.create_map(f.lit(0), f.lit("negative"), f.lit(1), f.lit("positive"))
        feature_matrix = L2GFeatureMatrix(
            _df=train_sdf.union(test_sdf).withColumn(
                "goldStandardSet",
                f.coalesce(
                    label_map[f.col("goldStandardSet").cast("int")],
                    f.col("goldStandardSet").cast("string"),
                ),
            ),
            with_gold_standard=True,
        ).select_features(self.features_list)

        presplit_train_df = train_sdf.toPandas()
        presplit_test_df = test_sdf.toPandas()
        train_sdf.unpersist()
        test_sdf.unpersist()

        # Run the training
        trained_model = LocusToGeneTrainer(
            model=l2g_model, feature_matrix=feature_matrix
        ).train(
            wandb_run_name=self.wandb_run_name,
            train_on_full_dataset=self.train_on_full_dataset,
            presplit_train_df=presplit_train_df,
            presplit_test_df=presplit_test_df,
        )

        # Export the model
        if trained_model.training_data and trained_model.model and self.model_path:
            trained_model.save(self.model_path)
            if self.hf_hub_repo_id and self.hf_model_commit_message and hf_token:
                trained_model.export_to_hugging_face_hub(
                    self.model_path.split("/")[-1],
                    hf_token=hf_token,
                    feature_matrix=trained_model.training_data,
                    repo_id=self.hf_hub_repo_id,
                    commit_message=self.hf_model_commit_message,
                )


class LocusToGeneModelTuningStep:
    """Run L2G cross-validation on pre-built, annotated train and test feature matrices.

    Unlike LocusToGeneStep, this step does not construct the training set or perform
    any gold-standard parsing. It expects parquet files that already contain the
    feature columns and the ``goldStandardSet`` label column.
    """

    def __init__(
        self,
        session: Session,
        *,
        train_feature_matrix_path: str,
        test_feature_matrix_path: str,
        hyperparameters: dict[str, Any],
        features_list: list[str] | None = None,
        n_splits: int = 5,
        hyperparameter_grid: dict[str, Any] | None = None,
        cv_results_dir: str | None = None,
        holdout_only: bool = False,
    ) -> None:
        """Initialise the step and run cross-validation.

        Args:
            session (Session): Session object that contains the Spark session
            train_feature_matrix_path (str): Path to the parquet file containing the
                annotated training feature matrix.  Must include ``credibleSetId``,
                ``geneId``, a ``goldStandardSet`` column with values ``"positive"``/
                ``"negative"`` (strings) or ``1``/``0`` (integers), and one column per feature.
            test_feature_matrix_path (str): Path to the parquet file containing the
                annotated test feature matrix with the same schema as the training file.
            hyperparameters (dict[str, Any]): Hyperparameters passed to XGBClassifier.
            features_list (list[str] | None): Subset of feature columns to use.  When
                ``None`` every column that is not a fixed metadata column is used.
            n_splits (int): Number of cross-validation folds. Defaults to 5.
            hyperparameter_grid (dict[str, Any] | None): Grid to sweep over in the form
                ``{"param": {"values": [v1, v2, ...]}, ...}``.  When provided with
                ``cv_results_dir``, all configs are evaluated and written to file.
                Defaults to None (CV uses the fixed ``hyperparameters``).
            cv_results_dir (str | None): Directory (local or ``gs://``) to write
                ``cv_results.json``, ``cv_folds.csv``, and per-config plots.  When None,
                metrics are logged to the terminal. Defaults to None.
            holdout_only (bool): When True, skip CV folds and evaluate each config directly
                on the holdout set. Requires ``cv_results_dir`` and a non-empty test set.
                Defaults to False.
        """
        train_feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(train_feature_matrix_path, "parquet"),
            features_list=features_list,
            with_gold_standard=True,
        ).persist()

        test_feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(test_feature_matrix_path, "parquet"),
            features_list=features_list,
            with_gold_standard=True,
        ).persist()

        effective_features = train_feature_matrix.features_list

        l2g_model = LocusToGeneModel(
            model=XGBClassifier(random_state=777, eval_metric="aucpr"),
            hyperparameters=hyperparameters,
            features_list=effective_features,
        )

        trainer = LocusToGeneTrainer(
            model=l2g_model,
            feature_matrix=train_feature_matrix,
        )

        # Convert to pandas and encode labels, mirroring generate_train_test_split
        label_col = train_feature_matrix.label_col
        train_pd: pd.DataFrame = train_feature_matrix._df.toPandas()
        test_pd: pd.DataFrame = test_feature_matrix._df.toPandas()
        train_feature_matrix._df.unpersist()
        test_feature_matrix._df.unpersist()
        # The split step saves goldStandardSet as integers 0/1; only encode if still strings.
        for df in (train_pd, test_pd):
            if df[label_col].dtype == object:
                df[label_col] = df[label_col].map(l2g_model.label_encoder)

        x_train: np.ndarray = (
            train_pd[effective_features].apply(pd.to_numeric).to_numpy()
        )
        y_train: np.ndarray = train_pd[label_col].apply(pd.to_numeric).to_numpy()
        x_test: np.ndarray = (
            test_pd[effective_features].apply(pd.to_numeric).to_numpy()
        )
        y_test: np.ndarray = test_pd[label_col].apply(pd.to_numeric).to_numpy()

        trainer.train_df = train_pd
        trainer.test_df = test_pd
        trainer.x_train = x_train
        trainer.y_train = y_train
        trainer.x_test = x_test
        trainer.y_test = y_test

        trainer.cross_validate(
            parameter_grid=hyperparameter_grid,
            n_splits=n_splits,
            cv_results_dir=cv_results_dir,
            holdout_only=holdout_only,
        )

        trainer.fit()

        trainer.log_to_terminal(
            eval_id="Hold-out",
            metrics=trainer.evaluate(
                y_true=y_test,
                y_pred=trainer.model.model.predict(x_test),
                y_pred_proba=trainer.model.model.predict_proba(x_test),
            ),
        )
