"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
from sklearn.ensemble import GradientBoostingClassifier
from wandb import login as wandb_login

from gentropy.common.session import Session
from gentropy.common.utils import access_gcp_secret
from gentropy.config import LocusToGeneConfig
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature_matrix import L2GFeatureInputLoader, L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.v2g import V2G
from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.method.l2g.trainer import LocusToGeneTrainer


class LocusToGeneStep:
    """Locus to gene step."""

    def __init__(
        self,
        session: Session,
        hyperparameters: dict[str, Any],
        *,
        run_mode: str,
        features_list: list[str],
        download_from_hub: bool,
        wandb_run_name: str,
        model_path: str | None = None,
        credible_set_path: str,
        gold_standard_curation_path: str | None = None,
        variant_gene_path: str | None = None,
        colocalisation_path: str | None = None,
        study_index_path: str | None = None,
        gene_interactions_path: str | None = None,
        predictions_path: str | None = None,
        feature_matrix_path: str | None = None,
        hf_hub_repo_id: str | None = LocusToGeneConfig().hf_hub_repo_id,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            hyperparameters (dict[str, Any]): Hyperparameters for the model
            run_mode (str): Run mode, either 'train' or 'predict'
            features_list (list[str]): List of features to use for the model
            download_from_hub (bool): Whether to download the model from Hugging Face Hub
            wandb_run_name (str): Name of the run to track model training in Weights and Biases
            model_path (str | None): Path to the model. It can be either in the filesystem or the name on the Hugging Face Hub (in the form of username/repo_name).
            credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
            gold_standard_curation_path (str | None): Path to the gold standard curation file
            variant_gene_path (str | None): Path to the variant-gene dataset
            colocalisation_path (str | None): Path to the colocalisation dataset
            study_index_path (str | None): Path to the study index dataset
            gene_interactions_path (str | None): Path to the gene interactions dataset
            predictions_path (str | None): Path to the L2G predictions output dataset
            feature_matrix_path (str | None): Path to the L2G feature matrix output dataset
            hf_hub_repo_id (str | None): Hugging Face Hub repository ID. If provided, the model will be uploaded to Hugging Face.

        Raises:
            ValueError: If run_mode is not 'train' or 'predict'
        """
        if run_mode not in ["train", "predict"]:
            raise ValueError(
                f"run_mode must be one of 'train' or 'predict', got {run_mode}"
            )

        self.session = session
        self.run_mode = run_mode
        self.model_path = model_path
        self.predictions_path = predictions_path
        self.features_list = list(features_list)
        self.hyperparameters = dict(hyperparameters)
        self.feature_matrix_path = feature_matrix_path
        self.wandb_run_name = wandb_run_name
        self.hf_hub_repo_id = hf_hub_repo_id
        self.download_from_hub = download_from_hub

        # Load common inputs
        self.credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        self.studies = (
            StudyIndex.from_parquet(session, study_index_path, recursiveFileLookup=True)
            if study_index_path
            else None
        )
        self.v2g = (
            V2G.from_parquet(session, variant_gene_path) if variant_gene_path else None
        )
        self.coloc = (
            Colocalisation.from_parquet(
                session, colocalisation_path, recursiveFileLookup=True
            )
            if colocalisation_path
            else None
        )
        self.features_input_loader = L2GFeatureInputLoader(
            v2g=self.v2g,
            coloc=self.coloc,
            studies=self.studies,
        )

        if run_mode == "predict":
            if not self.studies and self.v2g and self.coloc:
                raise ValueError("Dependencies for predict mode not set.")
            self.run_predict()
        elif run_mode == "train":
            if not gold_standard_curation_path and gene_interactions_path:
                raise ValueError("Dependencies for train mode not set.")
            self.gs_curation = self.session.spark.read.json(gold_standard_curation_path)
            self.interactions = self.session.spark.read.parquet(gene_interactions_path)
            self.run_train()

    def run_predict(self) -> None:
        """Run the prediction step.

        Raises:
            ValueError: If predictions_path is not set.
        """
        if not self.predictions_path:
            raise ValueError("predictions_path must be set for predict mode.")
        # TODO: IMPROVE - it is not correct that L2GPrediction outputs a feature matrix - FM should be written when training
        predictions, feature_matrix = L2GPrediction.from_credible_set(
            # TODO: rewrite this function to use the new FM generation
            self.features_list,
            self.credible_set,
            self.studies,
            self.v2g,
            self.coloc,
            self.session,
            model_path=self.model_path,
            hf_token=access_gcp_secret("hfhub-key", "open-targets-genetics-dev"),
            download_from_hub=self.download_from_hub,
        )
        if self.feature_matrix_path:
            feature_matrix._df.write.mode(self.session.write_mode).parquet(
                self.feature_matrix_path
            )
        predictions.df.write.mode(self.session.write_mode).parquet(
            self.predictions_path
        )
        self.session.logger.info(self.predictions_path)

    def run_train(self) -> None:
        """Run the training step.

        Raises:
            ValueError: If gold_standard_curation_path, gene_interactions_path, or wandb_run_name are not set.
        """
        if not (self.wandb_run_name and self.model_path):
            raise ValueError(
                "wandb_run_name, and a path to save the model must be set for train mode."
            )

        wandb_key = access_gcp_secret("wandb-key", "open-targets-genetics-dev")
        # Process gold standard and L2G features
        data = self._generate_feature_matrix()

        # Instantiate classifier and train model
        l2g_model = LocusToGeneModel(
            model=GradientBoostingClassifier(random_state=42),
            hyperparameters=self.hyperparameters,
        )
        wandb_login(key=wandb_key)
        trained_model = LocusToGeneTrainer(model=l2g_model, feature_matrix=data).train(
            self.wandb_run_name
        )
        if trained_model.training_data and trained_model.model:
            trained_model.save(self.model_path)
            if self.hf_hub_repo_id:
                hf_hub_token = access_gcp_secret(
                    "hfhub-key", "open-targets-genetics-dev"
                )
                trained_model.export_to_hugging_face_hub(
                    # we upload the model in the filesystem
                    self.model_path.split("/")[-1],
                    hf_hub_token,
                    data=trained_model.training_data._df.drop(
                        "goldStandardSet", "geneId"
                    ).toPandas(),
                    repo_id=self.hf_hub_repo_id,
                    commit_message="chore: update model",
                )

    def _generate_feature_matrix(self) -> L2GFeatureMatrix:
        """Generate the feature matrix for training.

        Returns:
            L2GFeatureMatrix: Feature matrix with gold standards annotated with features.
        """
        study_locus_overlap = StudyLocus(
            _df=self.credible_set.df.join(
                f.broadcast(
                    self.gs_curation.select(
                        StudyLocus.assign_study_locus_id(
                            f.col("association_info.otg_id"),  # studyId
                            f.concat_ws(  # variantId
                                "_",
                                f.col("sentinel_variant.locus_GRCh38.chromosome"),
                                f.col("sentinel_variant.locus_GRCh38.position"),
                                f.col("sentinel_variant.alleles.reference"),
                                f.col("sentinel_variant.alleles.alternative"),
                            ),
                        ).alias("studyLocusId"),
                    )
                ),
                "studyLocusId",
                "inner",
            ),
            _schema=StudyLocus.get_schema(),
        ).find_overlaps(self.studies)

        gold_standards = L2GGoldStandard.from_otg_curation(
            gold_standard_curation=self.gs_curation,
            v2g=self.v2g,
            study_locus_overlap=study_locus_overlap,
            interactions=self.interactions,
        )

        # TODO: Should StudyLocus and GoldStandard have an `annotate_w_features` method?
        fm = L2GFeatureMatrix.from_features_list(
            self.session, self.features_list, self.features_input_loader
        )

        return (
            L2GFeatureMatrix(
                _df=fm._df.join(
                    f.broadcast(
                        gold_standards.df.drop("variantId", "studyId", "sources")
                    ),
                    on=["studyLocusId", "geneId"],
                    how="inner",
                ),
            )
            .fill_na()
            .select_features(self.features_list)
        )
