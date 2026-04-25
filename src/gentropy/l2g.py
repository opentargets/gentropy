"""Step to run Locus to Gene either for inference or for training."""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pyspark.sql.functions as f
from pydantic import BaseModel, Field
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
from gentropy.external.gcs import access_gcp_secret
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader
from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.method.l2g.trainer import LocusToGeneTrainer


class LocusToGeneEvidenceDefaults(BaseModel, frozen=True):
    """Defaults for LocusToGeneEvidenceStep.

    All values are frozen - create a new instance to override.
    """

    locus_to_gene_predictions_path: Annotated[
        str, Field(description="Path to the L2G predictions dataset.")
    ]
    credible_set_path: Annotated[
        str, Field(description="Path to the credible set dataset.")
    ]
    study_index_path: Annotated[
        str, Field(description="Path to the study index dataset.")
    ]
    evidence_output_path: Annotated[
        str,
        Field(
            description="Path to the L2G evidence output dataset. The output format is ndjson gzipped."
        ),
    ]
    locus_to_gene_threshold: Annotated[
        float,
        Field(description="Threshold to consider a gene as a target.", default=0.05),
    ]


class LocusToGeneAssociationsDefaults(BaseModel, frozen=True):
    """Defaults for LocusToGeneAssociationsStep.

    All values are frozen - create a new instance to override.
    """

    evidence_input_path: Annotated[
        str, Field(description="Path to the L2G evidence input dataset.")
    ]
    disease_index_path: Annotated[str, Field(description="Path to disease index file.")]
    direct_associations_output_path: Annotated[
        str, Field(description="Path to the direct associations output dataset.")
    ]
    indirect_associations_output_path: Annotated[
        str, Field(description="Path to the indirect associations output dataset.")
    ]


class LocusToGeneFeatureMatrixDefaults(BaseModel, frozen=True):
    """Defaults for LocusToGeneFeatureMatrixStep.

    All values are frozen - create a new instance to override.
    """

    credible_set_path: Annotated[
        str,
        Field(
            description="Path to the credible set dataset necessary to build the feature matrix."
        ),
    ]
    feature_matrix_path: Annotated[
        str, Field(description="Path to the L2G feature matrix output dataset.")
    ]
    features_list: Annotated[
        list[str], Field(description="List of features to use for the model.")
    ]
    variant_index_path: Annotated[
        str | None,
        Field(description="Path to the variant index dataset.", default=None),
    ] = None
    colocalisation_path: Annotated[
        str | None,
        Field(description="Path to the colocalisation dataset.", default=None),
    ] = None
    study_index_path: Annotated[
        str | None,
        Field(description="Path to the study index dataset.", default=None),
    ] = None
    target_index_path: Annotated[
        str | None,
        Field(description="Path to the target index dataset.", default=None),
    ] = None
    intervals_path: Annotated[
        str | None,
        Field(description="Path to the interval dataset.", default=None),
    ] = None
    append_null_features: Annotated[
        bool,
        Field(
            description="Whether to append null features to the feature matrix.",
            default=False,
        ),
    ]


class LocusToGeneDefaults(BaseModel, frozen=True):
    """Defaults for LocusToGeneStep.

    All values are frozen - create a new instance to override.
    """

    run_mode: Annotated[
        str, Field(description="Run mode, either 'train' or 'predict'.")
    ]
    credible_set_path: Annotated[
        str,
        Field(
            description="Path to the credible set dataset necessary to build the feature matrix."
        ),
    ]
    feature_matrix_path: Annotated[
        str, Field(description="Path to the L2G feature matrix input dataset.")
    ]
    hyperparameters: Annotated[
        dict[str, Any], Field(description="Hyperparameters for the model.")
    ]
    download_from_hub: Annotated[
        bool, Field(description="Whether to download the model from Hugging Face Hub.")
    ]
    cross_validate: Annotated[
        bool,
        Field(
            description="Whether to run cross validation (5-fold by default) to train the model."
        ),
    ]
    wandb_run_name: Annotated[
        str | None,
        Field(
            description="Name of the run to track model training in Weights and Biases.",
            default=None,
        ),
    ] = None
    model_path: Annotated[
        str | None,
        Field(description="Path to the model.", default=None),
    ] = None
    features_list: Annotated[
        list[str] | None,
        Field(description="List of features to use to train the model.", default=None),
    ] = None
    gold_standard_curation_path: Annotated[
        str | None,
        Field(description="Path to the gold standard curation file.", default=None),
    ] = None
    variant_index_path: Annotated[
        str | None,
        Field(description="Path to the variant index.", default=None),
    ] = None
    gene_interactions_path: Annotated[
        str | None,
        Field(description="Path to the gene interactions dataset.", default=None),
    ] = None
    predictions_path: Annotated[
        str | None,
        Field(description="Path to the L2G predictions output dataset.", default=None),
    ] = None
    l2g_threshold: Annotated[
        float | None,
        Field(
            description="An optional threshold for the L2G score to filter predictions.",
            default=None,
        ),
    ] = None
    hf_hub_repo_id: Annotated[
        str | None,
        Field(description="Hugging Face Hub repository ID.", default=None),
    ] = None
    hf_model_commit_message: Annotated[
        str | None,
        Field(
            description="Commit message when we upload the model to the Hugging Face Hub.",
            default="chore: update model",
        ),
    ] = None
    hf_model_version: Annotated[
        str | None,
        Field(
            description="Tag, branch, or commit hash to download the model from the Hub.",
            default=None,
        ),
    ] = None
    explain_predictions: Annotated[
        bool | None,
        Field(
            description="Whether to extract SHAP importances for the L2G predictions.",
            default=None,
        ),
    ] = None


class LocusToGeneFeatureMatrixStep:
    """Annotate credible set with functional genomics features."""

    def __init__(
        self,
        session: Session,
        config: LocusToGeneFeatureMatrixDefaults,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            config: Configuration for the step.
        """
        credible_set = StudyLocus.from_parquet(
            session, config.credible_set_path, recursiveFileLookup=True
        )
        studies = (
            StudyIndex.from_parquet(
                session, config.study_index_path, recursiveFileLookup=True
            )
            if config.study_index_path
            else None
        )
        variant_index = (
            VariantIndex.from_parquet(session, config.variant_index_path)
            if config.variant_index_path
            else None
        )
        coloc = (
            Colocalisation.from_parquet(
                session, config.colocalisation_path, recursiveFileLookup=True
            )
            if config.colocalisation_path
            else None
        )

        target_index = (
            TargetIndex.from_parquet(
                session, config.target_index_path, recursiveFileLookup=True
            )
            if config.target_index_path
            else None
        )

        intervals = (
            Intervals.from_parquet(
                session, config.intervals_path, recursiveFileLookup=True
            )
            if config.intervals_path
            else None
        )

        features_input_loader = L2GFeatureInputLoader(
            variant_index=variant_index,
            colocalisation=coloc,
            study_index=studies,
            study_locus=credible_set,
            target_index=target_index,
            intervals=intervals,
        )

        fm = credible_set.filter(f.col("studyType") == "gwas").build_feature_matrix(
            config.features_list,
            features_input_loader,
            append_null_features=config.append_null_features,
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
        ).parquet(config.feature_matrix_path)


class LocusToGeneStep:
    """Locus to gene step."""

    def __init__(
        self,
        session: Session,
        config: LocusToGeneDefaults,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            config: Configuration for the step.

        Raises:
            ValueError: If run_mode is not 'train' or 'predict'
        """
        if config.run_mode not in ["train", "predict"]:
            raise ValueError(
                f"run_mode must be one of 'train' or 'predict', got {config.run_mode}"
            )

        self.session = session
        self.run_mode = config.run_mode
        self.predictions_path = config.predictions_path
        self.features_list = (
            list(config.features_list) if config.features_list else None
        )
        self.hyperparameters = dict(config.hyperparameters)
        self.wandb_run_name = config.wandb_run_name
        self.cross_validate = config.cross_validate
        self.hf_hub_repo_id = config.hf_hub_repo_id
        self.download_from_hub = config.download_from_hub
        self.hf_model_commit_message = config.hf_model_commit_message
        self.l2g_threshold = config.l2g_threshold or 0.0
        self.gold_standard_curation_path = config.gold_standard_curation_path
        self.gene_interactions_path = config.gene_interactions_path
        self.variant_index_path = config.variant_index_path
        self.model_path = (
            config.hf_hub_repo_id
            if not config.model_path
            and config.download_from_hub
            and config.hf_hub_repo_id
            else config.model_path
        )
        self.hf_model_version = config.hf_model_version
        self.explain_predictions = config.explain_predictions

        # Load common inputs
        self.credible_set = StudyLocus.from_parquet(
            session, config.credible_set_path, recursiveFileLookup=True
        )
        self.feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(config.feature_matrix_path, "parquet"),
        )

        if self.run_mode == "predict":
            self.run_predict()
        elif self.run_mode == "train":
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
                    self.gene_interactions_path, "parquet"
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
        if not self.predictions_path:
            raise ValueError("predictions_path must be provided for prediction mode")
        predictions = (
            L2GPrediction.from_credible_set(
                self.session,
                self.credible_set,
                self.feature_matrix,
                model_path=self.model_path,
                features_list=self.features_list,
                hf_token=self._get_hf_token(),
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

    def _get_hf_token(self) -> str | None:
        if self.download_from_hub:
            return access_gcp_secret("hfhub-key", "open-targets-genetics-dev")
        return None

    def run_train(self) -> None:
        """Run the training step.

        Raises:
            ValueError: If features list is not provided for model training.
        """
        if self.features_list is None:
            raise ValueError("Features list is required for model training.")
        # Initialize access to weights and biases
        if self.wandb_run_name:
            wandb_key = access_gcp_secret("wandb-key", "open-targets-genetics-dev")
            wandb_login(key=wandb_key)

        # Instantiate classifier and train model
        l2g_model = LocusToGeneModel(
            model=XGBClassifier(random_state=777, eval_metric="aucpr"),
            hyperparameters=self.hyperparameters,
            features_list=self.features_list,
        )

        # Calculate the gold standard features
        feature_matrix = self._annotate_gold_standards_w_feature_matrix()

        # Run the training
        trained_model = LocusToGeneTrainer(
            model=l2g_model, feature_matrix=feature_matrix
        ).train(wandb_run_name=self.wandb_run_name, cross_validate=self.cross_validate)

        # Export the model
        if trained_model.training_data and trained_model.model and self.model_path:
            trained_model.save(self.model_path)
            if self.hf_hub_repo_id and self.hf_model_commit_message:
                hf_hub_token = access_gcp_secret(
                    "hfhub-key", "open-targets-genetics-dev"
                )
                trained_model.export_to_hugging_face_hub(
                    # we upload the model saved in the filesystem
                    self.model_path.split("/")[-1],
                    hf_hub_token,
                    feature_matrix=trained_model.training_data,
                    repo_id=self.hf_hub_repo_id,
                    commit_message=self.hf_model_commit_message,
                )

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
        config: LocusToGeneEvidenceDefaults,
    ) -> None:
        """Initialise the step and generate disease/target evidence.

        Args:
            session (Session): Session object that contains the Spark session
            config: Configuration for the step.
        """
        # Reading the predictions
        locus_to_gene_prediction = L2GPrediction.from_parquet(
            session, config.locus_to_gene_predictions_path
        )
        # Reading the credible set
        credible_sets = StudyLocus.from_parquet(session, config.credible_set_path)

        # Reading the study index
        study_index = StudyIndex.from_parquet(session, config.study_index_path)

        # Generate evidence and save file:
        (
            locus_to_gene_prediction.to_disease_target_evidence(
                credible_sets, study_index, config.locus_to_gene_threshold
            )
            .coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(config.evidence_output_path)
        )


class LocusToGeneAssociationsStep:
    """Locus to gene associations step."""

    def __init__(
        self,
        session: Session,
        config: LocusToGeneAssociationsDefaults,
    ) -> None:
        """Create direct and indirect association datasets.

        Args:
            session (Session): Session object that contains the Spark session
            config: Configuration for the step.
        """
        # Read in the disease index
        disease_index = session.spark.read.parquet(config.disease_index_path).select(
            f.col("id").alias("diseaseId"),
            f.explode("ancestors").alias("ancestorDiseaseId"),
        )

        # Read in the L2G evidence
        disease_target_evidence = session.spark.read.json(
            config.evidence_input_path
        ).select(
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
            .parquet(config.direct_associations_output_path)
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
            .parquet(config.indirect_associations_output_path)
        )
