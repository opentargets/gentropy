"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

import json
import logging
from typing import Any

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
        credible_set_path: str,
        feature_matrix_path: str,
        wandb_run_name: str | None = None,
        model_path: str = "opentargets/locus_to_gene",
        features_list: list[str] | None = None,
        gold_standard_curation_path: str | None = None,
        variant_index_path: str | None = None,
        gene_interactions_path: str | None = None,
        predictions_path: str | None = None,
        l2g_threshold: float = 0.05,
        hf_hub_repo_id: str | None = "locus_to_gene",
        hf_model_commit_message: str = "chore: update model",
        hf_model_version: str | None = None,
        explain_predictions: bool = False,
        hyperparameter_grid: dict[str, Any] | None = None,
        cv_results_dir: str | None = None,
        hf_credentials_path: str | None = None,
        wandb_credentials_path: str | None = None,
        filter_dark_matter: bool = False,
        soft_label_weights: dict[str, float] | None = None,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            run_mode (str): Run mode, either 'train' or 'predict'
            hyperparameters (dict[str, Any]): Hyperparameters for the model
            download_from_hub (bool): Whether to download the model from Hugging Face Hub
            cross_validate (bool): Whether to run cross validation (5-fold by default) to train the model.
            train_on_full_dataset (bool): Whether to retrain the final saved model on the full dataset (train + held-out) after evaluation. Follows the standard practice of reporting honest held-out metrics while ensuring the deployed model benefits from all available labelled data.
            credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
            feature_matrix_path (str): Path to the L2G feature matrix input dataset
            wandb_run_name (str | None): Name of the run to track model training in Weights and Biases
            model_path (str): Path to the model. It can be either in the filesystem or the name on the Hugging Face Hub (in the form of username/repo_name).
            features_list (list[str] | None): List of features to use to train the model
            gold_standard_curation_path (str | None): Path to the gold standard curation file
            variant_index_path (str | None): Path to the variant index
            gene_interactions_path (str | None): Path to the protein-protein interaction (PPI) dataset
            predictions_path (str | None): Path to the L2G predictions output dataset
            l2g_threshold (float): An optional threshold for the L2G score to filter predictions. A threshold of 0.05 is recommended.
            hf_hub_repo_id (str | None): Hugging Face Hub repository handle in ``username/repo_name`` format. Used to download the model when ``download_from_hub`` is ``True`` (predict mode) and to upload the trained model (train mode). Set to None to skip HF Hub upload in train mode.
            hf_model_commit_message (str): Commit message when we upload the model to the Hugging Face Hub
            hf_model_version (str | None): Tag, branch, or commit hash to download the model from the Hub. Defaults to latest commit when provided None.
            explain_predictions (bool): Whether to extract SHAP importances for the L2G predictions. This is computationally expensive.
            hyperparameter_grid (dict[str, Any] | None): Hyperparameter grid to sweep over during cross-validation. Each key maps to {"values": [v1, v2, ...]}. If None, uses the fixed hyperparameters.
            cv_results_dir (str | None): Local directory to write CV results (JSON, CSV, plots). Only used when cross_validate=True and wandb_run_name is not set.
            hf_credentials_path (str | None): Optional path to the Hugging Face Hub credentials JSON file. If not provided, the HF_TOKEN environment variable will be used.
            wandb_credentials_path (str | None): Optional path to the Weights and Biases credentials JSON file. If not provided, the WANDB_API_KEY environment variable will be used.
            filter_dark_matter (bool): Whether to remove loci where every gold-standard positive has no functional genomics signal (zero QTL colocalisation, E2G, and VEP below protein-altering threshold) and is not the nearest gene (all neighbourhood distance features < 1.0). Loci with at least one signal-carrying or nearest-gene positive are kept. Defaults to False.
            soft_label_weights (dict[str, float] | None): When set, replace binary gold-standard labels with soft confidence values. Keys and defaults: ``nearest_fgs=1.0``, ``not_nearest_fgs=1.0``, ``nearest_no_fgs=0.5``, ``not_nearest_no_fgs=0.1``. For TP/FP/TN/FN computation labels >= 0.5 are treated as positive. Defaults to None (hard binary labels).

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
        self.gold_standard_curation_path = gold_standard_curation_path
        self.gene_interactions_path = gene_interactions_path
        self.variant_index_path = variant_index_path
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
        self.hyperparameter_grid = hyperparameter_grid
        self.cv_results_dir = cv_results_dir
        self.filter_dark_matter = filter_dark_matter
        self.soft_label_weights = soft_label_weights

        # Load common inputs
        self.credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        self.feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(feature_matrix_path, "parquet"),
        )

        if run_mode == "predict":
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
            self.gold_standard = self.prepare_gold_standard()
            self.run_train()

    def prepare_gold_standard(self) -> L2GGoldStandard:
        """Prepare the gold standard for training.

        Returns:
            L2GGoldStandard: training dataset.

        Raises:
            ValueError: When gold standard path, is not provided, or when
                parsing OTG gold standard but missing interactions and variant index paths.
            TypeError: When gold standard is not OTG gold standard nor L2GGoldStandard.

        """
        if self.gold_standard_curation_path is None:
            raise ValueError("Gold Standard is required for model training.")
        # Read the gold standard either from json or parquet, default to parquet if can not infer the format from extension.
        ext = self.gold_standard_curation_path.split(".")[-1]
        ext = "parquet" if ext not in ["parquet", "json"] else ext
        gold_standard = self.session.load_data(self.gold_standard_curation_path, ext)
        schema_issues = compare_struct_schemas(
            gold_standard.schema, L2GGoldStandard.get_schema()
        )
        # Parse the gold standard depending on the input schema
        match schema_issues:
            case {**extra} if not extra:
                # Schema is the same as L2GGoldStandard - load the GS
                # NOTE: match to empty dict will be non-selective
                # see https://stackoverflow.com/questions/75389166/how-to-match-an-empty-dictionary                logging.info("Successfully parsed gold standard.")
                return L2GGoldStandard(
                    _df=gold_standard,
                    _schema=L2GGoldStandard.get_schema(),
                )
            case {"unexpected_columns": extra_columns}:
                # All mandatory columns present, extra columns are allowed but not passed to the L2GGoldStandard object
                logging.info("Successfully parsed gold standard with extra columns.")
                return L2GGoldStandard(
                    _df=gold_standard.drop(*extra_columns),
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
                # There are schema mismatches, this would mean that we have
                logging.info("Detected OTG Gold Standard. Attempting to parse it.")
                otg_curation = gold_standard
                if self.gene_interactions_path is None:
                    raise ValueError("Interactions are required for parsing curation.")
                if self.variant_index_path is None:
                    raise ValueError("Variant Index are required for parsing curation.")

                interactions = self.session.load_data(
                    self.gene_interactions_path, "parquet", recursiveFileLookup=True
                )
                variant_index = VariantIndex.from_parquet(
                    self.session, self.variant_index_path
                )
                study_locus_overlap = StudyLocus(
                    _df=self.credible_set.df.join(
                        otg_curation.select(
                            f.concat_ws(
                                "_",
                                f.col("sentinel_variant.locus_GRCh38.chromosome"),
                                f.col("sentinel_variant.locus_GRCh38.position"),
                                f.col("sentinel_variant.alleles.reference"),
                                f.col("sentinel_variant.alleles.alternative"),
                            ).alias("variantId"),
                            f.col("association_info.otg_id").alias("studyId"),
                        ),
                        on=[
                            "studyId",
                            "variantId",
                        ],
                        how="inner",
                    ),
                    _schema=StudyLocus.get_schema(),
                ).find_overlaps()

                return L2GGoldStandard.from_otg_curation(
                    gold_standard_curation=otg_curation,
                    variant_index=variant_index,
                    study_locus_overlap=study_locus_overlap,
                    interactions=interactions,
                )
            case _:
                raise TypeError("Incorrect gold standard dataset provided.")

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
            ValueError: If features list is not provided for model training.
        """
        if self.features_list is None:
            raise ValueError("Features list is required for model training.")
        # Initialize access to weights and biases
        if self.wandb_run_name:
            wandb_credentials = WandbCredentials.read(self.wandb_credentials_path)
            wandb_login(key=wandb_credentials.WANDB_API_KEY.get_secret_value())

        # Initialize access to Hugging Face Hub
        hf_token = None
        if self.hf_hub_repo_id and self.hf_model_commit_message:
            # Fails when the HF_TOKEN env is not set
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

        # Calculate the gold standard features
        feature_matrix = self._annotate_gold_standards_w_feature_matrix()

        if self.filter_dark_matter:
            feature_matrix, dark_matter_stats = feature_matrix.filter_dark_matter_loci()
            if dark_matter_stats:
                if self.model_path:
                    self._write_dark_matter_stats(dark_matter_stats)
                else:
                    logging.warning(
                        "filter_dark_matter=True but model_path is not set — "
                        "dark matter stats were computed but not written anywhere."
                    )
            else:
                logging.warning(
                    "filter_dark_matter=True but the filter had no effect: "
                    "required signal or neighbourhood-distance features are "
                    "absent from features_list. Verify that the dark matter "
                    "features are included in the training feature set."
                )

        # Determine effective soft label weights: explicit config takes priority;
        # fall back to defaults when soft label keys appear in the hyperparameter grid
        # (so _is_nearest / _has_fgs columns are available for per-fold re-application).
        from gentropy.method.l2g.trainer import _SOFT_LABEL_KEYS

        grid_has_soft_label_keys = bool(
            self.hyperparameter_grid
            and _SOFT_LABEL_KEYS & set(self.hyperparameter_grid.keys())
        )
        effective_soft_label_weights = self.soft_label_weights or (
            {} if grid_has_soft_label_keys else None
        )
        if effective_soft_label_weights is not None:
            feature_matrix = feature_matrix.apply_soft_labels(
                **effective_soft_label_weights
            )
            logging.info("Soft labels applied: %s", effective_soft_label_weights)

        # Run the training
        trained_model = LocusToGeneTrainer(
            model=l2g_model, feature_matrix=feature_matrix
        ).train(
            wandb_run_name=self.wandb_run_name,
            cross_validate=self.cross_validate,
            train_on_full_dataset=self.train_on_full_dataset,
            hyperparameter_grid=self.hyperparameter_grid,
            cv_results_dir=self.cv_results_dir,
        )

        # Export the model
        if trained_model.training_data and trained_model.model and self.model_path:
            trained_model.save(self.model_path)
            if self.hf_hub_repo_id and self.hf_model_commit_message and hf_token:
                trained_model.export_to_hugging_face_hub(
                    # we upload the model saved in the filesystem
                    self.model_path.split("/")[-1],
                    hf_token=hf_token,
                    feature_matrix=trained_model.training_data,
                    repo_id=self.hf_hub_repo_id,
                    commit_message=self.hf_model_commit_message,
                )

    def _write_dark_matter_stats(self, stats: dict[str, Any]) -> None:
        """Write dark matter filtering stats as JSON next to the model path.

        Args:
            stats (dict[str, Any]): Stats dict returned by filter_dark_matter_loci.
        """
        from urllib.parse import urlparse

        from google.cloud import storage

        log_path = f"{self.model_path}_dark_matter_stats.json"
        payload = json.dumps(stats, indent=2)
        if log_path.startswith("gs://"):
            parsed = urlparse(log_path)
            bucket = storage.Client().bucket(parsed.hostname)
            bucket.blob(parsed.path.lstrip("/")).upload_from_string(
                payload, content_type="application/json"
            )
        else:
            from pathlib import Path

            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "w") as fh:
                fh.write(payload)
        logging.info("Dark matter filter stats written to %s", log_path)

    def _annotate_gold_standards_w_feature_matrix(self) -> L2GFeatureMatrix:
        """Generate the feature matrix of annotated gold standards.

        Returns:
            L2GFeatureMatrix: Feature matrix with gold standards annotated with features.
        """
        return (
            self.gold_standard.build_feature_matrix(
                self.feature_matrix, self.credible_set
            )
            .select_features(self.features_list)
            .persist()
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
