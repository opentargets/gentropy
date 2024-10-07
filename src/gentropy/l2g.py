"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

from functools import reduce
from typing import Any

import pyspark.sql.functions as f
from sklearn.ensemble import GradientBoostingClassifier
from wandb import login as wandb_login

from gentropy.common.Liftover import LiftOverSpark
from gentropy.common.session import Session
from gentropy.common.utils import access_gcp_secret
from gentropy.config import LocusToGeneConfig
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader
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
        variant_index_path: str | None = None,
        colocalisation_path: str | None = None,
        study_index_path: str | None = None,
        gene_index_path: str | None = None,
        gene_interactions_path: str | None = None,
        interval_path: dict[str, str] | None = None,
        liftover_chain_file_path: str | None = None,
        liftover_max_length_difference: int = 100,
        predictions_path: str | None = None,
        feature_matrix_path: str | None = None,
        write_feature_matrix: bool,
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
            variant_index_path (str | None): Path to the variant index dataset
            colocalisation_path (str | None): Path to the colocalisation dataset
            study_index_path (str | None): Path to the study index dataset
            gene_index_path (str | None): Path to the gene index dataset
            gene_interactions_path (str | None): Path to the gene interactions dataset
            interval_path (dict[str, str] | None) : Path and source of interval input datasets
            liftover_chain_file_path (str | None) : Path to the liftover chain file
            liftover_max_length_difference (int) : Maximum allowed difference for liftover
            predictions_path (str | None): Path to the L2G predictions output dataset
            feature_matrix_path (str | None): Path to the L2G feature matrix output dataset
            write_feature_matrix (bool): Whether to write the full feature matrix to the filesystem
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
        ).filter(f.col("studyType") == "gwas")
        self.studies = (
            StudyIndex.from_parquet(session, study_index_path, recursiveFileLookup=True)
            if study_index_path
            else None
        )
        self.variant_index = (
            VariantIndex.from_parquet(session, variant_index_path)
            if variant_index_path
            else None
        )
        self.gene_index = (
            GeneIndex.from_parquet(session, gene_index_path)
            if gene_index_path
            else None
        )
        self.lift = (
            LiftOverSpark(
                liftover_chain_file_path,
                liftover_max_length_difference,
            )
            if liftover_chain_file_path
            else None
        )

        if self.variant_index and self.gene_index and self.lift and interval_path:
            self.intervals = Intervals(
                _df=reduce(
                    lambda x, y: x.unionByName(y, allowMissingColumns=True),
                    # create interval instances by parsing each source
                    [
                        Intervals.from_source(
                            session.spark,
                            source_name,
                            source_path,
                            self.gene_index,
                            self.lift,
                        ).df
                        for source_name, source_path in interval_path.items()
                    ],
                )
                .alias("interval")
                .join(
                    self.variant_index.df.selectExpr(
                        "chromosome as vi_chromosome", "variantId", "position"
                    ).alias("vi"),
                    on=[
                        f.col("vi.vi_chromosome") == f.col("interval.chromosome"),
                        f.col("vi.position").between(
                            f.col("interval.start"), f.col("interval.end")
                        ),
                    ],
                    how="inner",
                )
                .drop("start", "end", "vi_chromosome", "position"),
                _schema=Intervals.get_schema(),
            )
        else:
            raise ValueError("variant_index is None, cannot join with intervals.")
        self.coloc = (
            Colocalisation.from_parquet(
                session, colocalisation_path, recursiveFileLookup=True
            )
            if colocalisation_path
            else None
        )
        self.gene_index = (
            GeneIndex.from_parquet(session, gene_index_path, recursiveFileLookup=True)
            if gene_index_path
            else None
        )
        self.features_input_loader = L2GFeatureInputLoader(
            variant_index=self.variant_index,
            coloc=self.coloc,
            studies=self.studies,
            study_locus=self.credible_set,
            gene_index=self.gene_index,
        )

        if run_mode == "predict":
            self.run_predict()
        elif run_mode == "train":
            self.gs_curation = (
                self.session.spark.read.json(gold_standard_curation_path)
                if gold_standard_curation_path
                else None
            )
            self.interactions = (
                self.session.spark.read.parquet(gene_interactions_path)
                if gene_interactions_path
                else None
            )
            self.run_train()

    def run_predict(self) -> None:
        """Run the prediction step.

        Raises:
            ValueError: If not all dependencies in prediction mode are set
        """
        if self.studies and self.coloc:
            predictions = L2GPrediction.from_credible_set(
                self.session,
                self.credible_set,
                self.features_list,
                self.features_input_loader,
                model_path=self.model_path,
                hf_token=access_gcp_secret("hfhub-key", "open-targets-genetics-dev"),
                download_from_hub=self.download_from_hub,
            )
            if self.predictions_path:
                predictions.df.write.mode(self.session.write_mode).parquet(
                    self.predictions_path
                )
                self.session.logger.info(self.predictions_path)
        else:
            raise ValueError("Dependencies for predict mode not set.")

    def run_train(self) -> None:
        """Run the training step."""
        if (
            self.gs_curation
            and self.interactions
            and self.wandb_run_name
            and self.model_path
            and self.variant_index
        ):
            wandb_key = access_gcp_secret("wandb-key", "open-targets-genetics-dev")
            # Process gold standard and L2G features
            data = self._generate_feature_matrix(write_feature_matrix=True)

            # Instantiate classifier and train model
            l2g_model = LocusToGeneModel(
                model=GradientBoostingClassifier(random_state=42),
                hyperparameters=self.hyperparameters,
            )
            wandb_login(key=wandb_key)
            trained_model = LocusToGeneTrainer(
                model=l2g_model, feature_matrix=data
            ).train(self.wandb_run_name)
            if trained_model.training_data and trained_model.model and self.model_path:
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

    def _generate_feature_matrix(self, write_feature_matrix: bool) -> L2GFeatureMatrix:
        """Generate the feature matrix of annotated gold standards.

        Args:
            write_feature_matrix (bool): Whether to write the feature matrix for all credible sets to disk

        Returns:
            L2GFeatureMatrix: Feature matrix with gold standards annotated with features.

        Raises:
            ValueError: If write_feature_matrix is set to True but a path is not provided or if dependencies to build features are not set.
        """
        if (
            self.gs_curation
            and self.interactions
            and self.studies
            and self.variant_index
        ):
            study_locus_overlap = StudyLocus(
                _df=self.credible_set.df.join(
                    f.broadcast(
                        self.gs_curation.withColumn(
                            "variantId",
                            f.concat_ws(
                                "_",
                                f.col("sentinel_variant.locus_GRCh38.chromosome"),
                                f.col("sentinel_variant.locus_GRCh38.position"),
                                f.col("sentinel_variant.alleles.reference"),
                                f.col("sentinel_variant.alleles.alternative"),
                            ),
                        ).select(
                            StudyLocus.assign_study_locus_id(
                                [
                                    "association_info.otg_id",  # studyId
                                    "variantId",
                                ]
                            ),
                        )
                    ),
                    "studyLocusId",
                    "inner",
                ),
                _schema=StudyLocus.get_schema(),
            ).find_overlaps()

            gold_standards = L2GGoldStandard.from_otg_curation(
                gold_standard_curation=self.gs_curation,
                variant_index=self.variant_index,
                study_locus_overlap=study_locus_overlap,
                interactions=self.interactions,
            )

            fm = self.credible_set.build_feature_matrix(
                self.features_list, self.features_input_loader
            )
            if write_feature_matrix:
                if not self.feature_matrix_path:
                    raise ValueError("feature_matrix_path must be set.")
                fm._df.write.mode(self.session.write_mode).parquet(
                    self.feature_matrix_path
                )

            return (
                gold_standards.build_feature_matrix(fm)
                .fill_na()
                .select_features(self.features_list)
            )
        raise ValueError("Dependencies for train mode not set.")
