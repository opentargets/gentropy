"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd
import pyspark.sql.functions as f
from wandb.sdk.wandb_login import login as wandb_login
from xgboost import XGBClassifier

from gentropy.common.schemas import compare_struct_schemas
from gentropy.common.session import Session
from gentropy.common.spark import calculate_harmonic_sum
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
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


class LocusToGeneFeatureMatrixStep:
    """Annotate credible set with functional genomics features."""

    def __init__(
        self,
        session: Session,
        *,
        features_list: list[str],
        credible_set_path: str,
        variant_index_path: str | None = None,
        colocalisation_path: str | None = None,
        study_index_path: str | None = None,
        target_index_path: str | None = None,
        intervals_path: str | None = None,
        gene_interactions_path: str | None = None,
        feature_matrix_path: str,
        append_null_features: bool = False,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            features_list (list[str]): List of features to use for the model
            credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
            variant_index_path (str | None): Path to the variant index dataset
            colocalisation_path (str | None): Path to the colocalisation dataset
            study_index_path (str | None): Path to the study index dataset
            target_index_path (str | None): Path to the target index dataset
            intervals_path (str | None): Path to the interval dataset
            gene_interactions_path (str | None): Path to the protein-protein interaction (PPI) dataset
            feature_matrix_path (str): Path to the L2G feature matrix output dataset
            append_null_features (bool): Whether to append null features to the feature matrix. Defaults to False.
        """
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
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

        features_input_loader = L2GFeatureInputLoader(
            variant_index=variant_index,
            colocalisation=coloc,
            study_index=studies,
            study_locus=credible_set,
            target_index=target_index,
            intervals=intervals,
            interactions=interactions,
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


class LocusToGeneTrainTestSplitStep:
    """Split the annotated gold standard feature matrix into train/test partitions and write to parquet."""

    def __init__(
        self,
        session: Session,
        *,
        credible_set_path: str,
        feature_matrix_path: str,
        gold_standard_curation_path: str,
        train_parquet_path: str,
        test_parquet_path: str,
        features_list: list[str],
        test_size: float = 0.15,
        variant_index_path: str | None = None,
        gene_interactions_path: str | None = None,
        predefined_test_parquet_path: str | None = None,
        split_stats_path: str | None = None,
    ) -> None:
        """Initialise step: build annotated feature matrix, split, and persist.

        Args:
            session (Session): Session object that contains the Spark session
            credible_set_path (str): Path to the credible set dataset
            feature_matrix_path (str): Path to the L2G feature matrix input dataset
            gold_standard_curation_path (str): Path to the gold standard curation file (parquet or JSON)
            train_parquet_path (str): Output path for the training split parquet
            test_parquet_path (str): Output path for the held-out test split parquet
            features_list (list[str]): Features to select from the feature matrix
            test_size (float): Proportion of study loci assigned to the test split. Defaults to 0.15.
            variant_index_path (str | None): Path to the variant index (required for OTG gold standard)
            gene_interactions_path (str | None): Path to the PPI dataset (required for OTG gold standard)
            predefined_test_parquet_path (str | None): Path to an existing test-split parquet (produced by a previous run of this step). When provided the test set is loaded as-is and the training set is derived by removing all studyLocusIds from the annotated feature matrix whose positive genes overlap with the test set's positive genes. ``test_size`` is ignored. Defaults to None (performs a fresh hierarchical split).
            split_stats_path (str | None): Explicit path for the split statistics JSON file. Defaults to ``<train_parquet_path>_split_stats.json``.
        """
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(feature_matrix_path, "parquet"),
        )
        gold_standard = self._parse_gold_standard(
            session=session,
            gold_standard_curation_path=gold_standard_curation_path,
            credible_set=credible_set,
            variant_index_path=variant_index_path,
            gene_interactions_path=gene_interactions_path,
        )

        # Build annotated feature matrix for gold standard loci
        annotated_fm = (
            gold_standard.build_feature_matrix(feature_matrix, credible_set)
            .select_features(features_list)
            .persist()
        )

        label_encoder = {"negative": 0, "positive": 1}

        if predefined_test_parquet_path:
            predefined_test_sdf = session.spark.read.parquet(predefined_test_parquet_path)

            n_original_total: int = annotated_fm._df.count()
            n_original_test: int = predefined_test_sdf.count()

            # Positive gene IDs from the predefined test set.
            test_positive_genes_sdf = predefined_test_sdf.filter(
                f.col("goldStandardSet").isin([1, "positive"])
            ).select("geneId").distinct()

            # studyLocusIds in annotated_fm that contain at least one test-positive gene.
            contaminating_sdf = (
                annotated_fm._df.join(test_positive_genes_sdf, on="geneId", how="inner")
                .filter(f.col("goldStandardSet") == "positive")
                .select("studyLocusId")
                .distinct()
            )

            # Train set: remove contaminating studyLocusIds entirely.
            train_sdf = annotated_fm._df.join(
                contaminating_sdf, on="studyLocusId", how="left_anti"
            )

            # Test set: re-derive features from current annotated_fm using the predefined pairs.
            test_pairs_sdf = predefined_test_sdf.select("studyLocusId", "geneId")
            test_sdf = annotated_fm._df.join(
                test_pairs_sdf, on=["studyLocusId", "geneId"], how="inner"
            )

            # Apply label encoding in Spark (handles both string and already-encoded int values).
            label_map = f.create_map(f.lit("negative"), f.lit(0), f.lit("positive"), f.lit(1))
            train_sdf = train_sdf.withColumn(
                "goldStandardSet",
                f.coalesce(label_map[f.col("goldStandardSet")], f.col("goldStandardSet").cast("int")),
            )
            test_sdf = test_sdf.withColumn(
                "goldStandardSet",
                f.coalesce(label_map[f.col("goldStandardSet")], f.col("goldStandardSet").cast("int")),
            )

            train_sdf = train_sdf.persist()
            test_sdf = test_sdf.persist()
            n_train: int = train_sdf.count()
            n_test_new: int = test_sdf.count()
            split_stats: dict[str, Any] = {
                "n_original_total": n_original_total,
                "n_original_test": n_original_test,
                "n_test_new": n_test_new,
                "n_lost_test": n_original_test - n_test_new,
                "n_train": n_train,
                "n_lost_total": n_original_total - n_test_new - n_train,
            }
            self._write_split_stats(split_stats, train_parquet_path, split_stats_path)
            logging.info("Split stats: %s", split_stats)
            train_sdf.write.mode(session.write_mode).parquet(train_parquet_path)
            test_sdf.write.mode(session.write_mode).parquet(test_parquet_path)
            train_sdf.unpersist()
            test_sdf.unpersist()
            n_written_train, n_written_test = n_train, n_test_new
        else:
            train_df, test_df = annotated_fm.generate_train_test_split(
                test_size=test_size,
                verbose=True,
                label_encoder=label_encoder,
                label_col=annotated_fm.label_col,
            )
            n_written_train, n_written_test = len(train_df), len(test_df)
            split_stats = {
                "n_train": n_written_train,
                "n_test": n_written_test,
                "test_size": test_size,
            }
            self._write_split_stats(split_stats, train_parquet_path, split_stats_path)
            logging.info("Split stats: %s", split_stats)
            session.spark.createDataFrame(train_df).write.mode(
                session.write_mode
            ).parquet(train_parquet_path)
            session.spark.createDataFrame(test_df).write.mode(
                session.write_mode
            ).parquet(test_parquet_path)

        annotated_fm._df.unpersist()
        logging.info(
            "Train/test split written: %d train rows, %d test rows.",
            n_written_train,
            n_written_test,
        )

    @staticmethod
    def _parse_gold_standard(
        session: Session,
        gold_standard_curation_path: str,
        credible_set: StudyLocus,
        variant_index_path: str | None,
        gene_interactions_path: str | None,
    ) -> L2GGoldStandard:
        """Load and parse the gold standard curation file into an L2GGoldStandard.

        Args:
            session (Session): Active Spark session.
            gold_standard_curation_path (str): Path to the gold standard curation file (parquet or JSON).
            credible_set (StudyLocus): Credible set used for OTG curation parsing.
            variant_index_path (str | None): Path to variant index (required for OTG format).
            gene_interactions_path (str | None): Path to PPI dataset (required for OTG format).

        Returns:
            L2GGoldStandard: Parsed gold standard dataset.

        Raises:
            ValueError: If OTG format is detected but required paths are missing.
            TypeError: If the gold standard schema is unrecognised.
        """
        ext = gold_standard_curation_path.rsplit(".", maxsplit=1)[-1]
        ext = "parquet" if ext not in ["parquet", "json"] else ext
        gold_standard_raw = session.load_data(gold_standard_curation_path, ext)
        schema_issues = compare_struct_schemas(
            gold_standard_raw.schema, L2GGoldStandard.get_schema()
        )
        match schema_issues:
            case {**extra} if not extra:
                return L2GGoldStandard(
                    _df=gold_standard_raw,
                    _schema=L2GGoldStandard.get_schema(),
                )
            case {"unexpected_columns": extra_columns} if "missing_mandatory_columns" not in schema_issues:
                return L2GGoldStandard(
                    _df=gold_standard_raw.drop(*extra_columns),
                    _schema=L2GGoldStandard.get_schema(),
                )
            case {
                "missing_mandatory_columns": [
                    "studyLocusId",
                    "variantId",
                    "studyId",
                    "geneId",
                    "goldStandardSet",
                ],
                "unexpected_columns": [
                    "association_info",
                    "gold_standard_info",
                    "metadata",
                    "sentinel_variant",
                    "trait_info",
                ],
            }:
                if gene_interactions_path is None:
                    raise ValueError("Interactions are required for parsing OTG curation.")
                if variant_index_path is None:
                    raise ValueError("Variant Index is required for parsing OTG curation.")
                interactions = session.load_data(
                    gene_interactions_path, "parquet", recursiveFileLookup=True
                )
                variant_index = VariantIndex.from_parquet(session, variant_index_path)
                study_locus_overlap = StudyLocus(
                    _df=credible_set.df.join(
                        gold_standard_raw.select(
                            f.concat_ws(
                                "_",
                                f.col("sentinel_variant.locus_GRCh38.chromosome"),
                                f.col("sentinel_variant.locus_GRCh38.position"),
                                f.col("sentinel_variant.alleles.reference"),
                                f.col("sentinel_variant.alleles.alternative"),
                            ).alias("variantId"),
                            f.col("association_info.otg_id").alias("studyId"),
                        ),
                        on=["studyId", "variantId"],
                        how="inner",
                    ),
                    _schema=StudyLocus.get_schema(),
                ).find_overlaps()
                return L2GGoldStandard.from_otg_curation(
                    gold_standard_curation=gold_standard_raw,
                    variant_index=variant_index,
                    study_locus_overlap=study_locus_overlap,
                    interactions=interactions,
                )
            case _:
                raise TypeError("Incorrect gold standard dataset provided.")

    @staticmethod
    def _write_split_stats(
        stats: dict[str, Any],
        train_parquet_path: str,
        split_stats_path: str | None = None,
    ) -> None:
        """Write split statistics as a JSON file.

        Args:
            stats (dict[str, Any]): Stats dict to serialise.
            train_parquet_path (str): Used to derive the default output path when ``split_stats_path`` is not provided.
            split_stats_path (str | None): Explicit destination path. Defaults to ``<train_parquet_path>_split_stats.json``.
        """
        from urllib.parse import urlparse

        log_path = split_stats_path or train_parquet_path.rstrip("/") + "_split_stats.json"
        payload = json.dumps(stats, indent=2)
        if log_path.startswith("gs://"):
            from google.cloud import storage

            parsed = urlparse(log_path)
            bucket = storage.Client().bucket(parsed.hostname)
            bucket.blob(parsed.path.lstrip("/")).upload_from_string(
                payload, content_type="application/json"
            )
        else:
            from pathlib import Path

            p = Path(log_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(payload)
        logging.info("Split stats written to %s", log_path)


class LocusToGeneStep:
    """Locus to gene step."""

    def __init__(
        self,
        session: Session,
        *,
        run_mode: str,
        hyperparameters: dict[str, Any],
        download_from_hub: bool,
        cross_validate: bool,
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
            cross_validate (bool): Whether to run cross validation (5-fold by default) to train the model.
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
            train_parquet_path (str | None): Path to the training split parquet produced by ``LocusToGeneTrainTestSplitStep``. Required in train mode.
            test_parquet_path (str | None): Path to the test split parquet produced by ``LocusToGeneTrainTestSplitStep``. Required in train mode.

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
        self.cross_validate = cross_validate

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
            self.credible_set = StudyLocus.from_parquet(
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
                "train_parquet_path and test_parquet_path are required for model training. "
                "Run LocusToGeneTrainTestSplitStep first."
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

        # Load presplit data produced by LocusToGeneTrainTestSplitStep
        presplit_train_df = self.session.spark.read.parquet(
            self.train_parquet_path
        ).toPandas()
        presplit_test_df = self.session.spark.read.parquet(
            self.test_parquet_path
        ).toPandas()

        # Reconstruct L2GFeatureMatrix for model metadata (column order, HF Hub upload).
        # Labels are decoded back to strings so generate_train_test_split works if
        # export_to_hugging_face_hub is called later.
        label_decoder = {0: "negative", 1: "positive"}
        combined = pd.concat([presplit_train_df, presplit_test_df], ignore_index=True)
        if combined["goldStandardSet"].dtype != object:
            combined["goldStandardSet"] = combined["goldStandardSet"].map(label_decoder)
        feature_matrix = L2GFeatureMatrix(
            _df=self.session.spark.createDataFrame(combined),
            with_gold_standard=True,
        ).select_features(self.features_list)

        # Run the training
        trained_model = LocusToGeneTrainer(
            model=l2g_model, feature_matrix=feature_matrix
        ).train(
            wandb_run_name=self.wandb_run_name,
            cross_validate=self.cross_validate,
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


class LocusToGeneEvidenceStep:
    """Locus to gene evidence step."""

    def __init__(
        self,
        session: Session,
        locus_to_gene_predictions_path: str,
        credible_set_path: str,
        study_index_path: str,
        evidence_output_path: str,
        locus_to_gene_threshold: float,
    ) -> None:
        """Initialise the step and generate disease/target evidence.

        Args:
            session (Session): Session object that contains the Spark session
            locus_to_gene_predictions_path (str): Path to the L2G predictions dataset
            credible_set_path (str): Path to the credible set dataset
            study_index_path (str): Path to the study index dataset
            evidence_output_path (str): Path to the L2G evidence output dataset. The output format is ndjson gzipped.
            locus_to_gene_threshold (float, optional): Threshold to consider a gene as a target. Defaults to 0.05.
        """
        # Reading the predictions
        locus_to_gene_prediction = L2GPrediction.from_parquet(
            session, locus_to_gene_predictions_path
        )
        # Reading the credible set
        credible_sets = StudyLocus.from_parquet(session, credible_set_path)

        # Reading the study index
        study_index = StudyIndex.from_parquet(session, study_index_path)

        # Generate evidence and save file:
        (
            locus_to_gene_prediction.to_disease_target_evidence(
                credible_sets, study_index, locus_to_gene_threshold
            )
            .coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(evidence_output_path)
        )


class LocusToGeneAssociationsStep:
    """Locus to gene associations step."""

    def __init__(
        self,
        session: Session,
        evidence_input_path: str,
        disease_index_path: str,
        direct_associations_output_path: str,
        indirect_associations_output_path: str,
    ) -> None:
        """Create direct and indirect association datasets.

        Args:
            session (Session): Session object that contains the Spark session
            evidence_input_path (str): Path to the L2G evidence input dataset
            disease_index_path (str): Path to disease index file
            direct_associations_output_path (str): Path to the direct associations output dataset
            indirect_associations_output_path (str): Path to the indirect associations output dataset
        """
        # Read in the disease index
        disease_index = session.spark.read.parquet(disease_index_path).select(
            f.col("id").alias("diseaseId"),
            f.explode("ancestors").alias("ancestorDiseaseId"),
        )

        # Read in the L2G evidence
        disease_target_evidence = session.spark.read.json(evidence_input_path).select(
            f.col("targetFromSourceId").alias("targetId"),
            f.col("diseaseFromSourceMappedId").alias("diseaseId"),
            f.col("resourceScore"),
        )

        # Generate direct assocations and save file
        (
            disease_target_evidence.groupBy("targetId", "diseaseId")
            .agg(f.collect_set("resourceScore").alias("scores"))
            .select(
                "targetId",
                "diseaseId",
                calculate_harmonic_sum(f.col("scores")).alias("harmonicSum"),
            )
            .write.mode(session.write_mode)
            .parquet(direct_associations_output_path)
        )

        # Generate indirect assocations and save file
        (
            disease_target_evidence.join(disease_index, on="diseaseId", how="inner")
            .groupBy("targetId", "ancestorDiseaseId")
            .agg(f.collect_set("resourceScore").alias("scores"))
            .select(
                "targetId",
                "ancestorDiseaseId",
                calculate_harmonic_sum(f.col("scores")).alias("harmonicSum"),
            )
            .write.mode(session.write_mode)
            .parquet(indirect_associations_output_path)
        )
