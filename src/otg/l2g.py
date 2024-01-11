"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
import sklearn
from xgboost.spark import SparkXGBClassifier

from otg.common.session import Session

# from otg.dataset.colocalisation import Colocalisation
from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.dataset.l2g_prediction import L2GPrediction
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G
from otg.method.l2g.model import LocusToGeneModel
from otg.method.l2g.trainer import LocusToGeneTrainer


class LocusToGeneStep:
    """Locus to gene step."""

    def __init__(
        self,
        session: Session,
        run_mode: str,
        model_path: str,
        predictions_path: str,
        credible_set_path: str,
        variant_gene_path: str,
        colocalisation_path: str,
        study_index_path: str,
        study_locus_overlap_path: str,
        gold_standard_curation_path: str,
        gene_interactions_path: str,
        features_list: list[str],
        hyperparameters: dict[str, Any],
        wandb_run_name: str | None = None,
        perform_cross_validation: bool = False,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            run_mode (str): One of "train" or "predict".
            model_path (str): Path to save the model.
            predictions_path (str): Path to save the predictions.
            credible_set_path (str): Path to credible set Parquet files.
            variant_gene_path (str): Path to variant to gene Parquet files.
            colocalisation_path (str): Path to colocalisation Parquet files.
            study_index_path (str): Path to study index Parquet files.
            study_locus_overlap_path (str): Path to study locus overlap Parquet files.
            gold_standard_curation_path (str): Path to gold standard curation JSON files.
            gene_interactions_path (str): Path to gene interactions Parquet files.
            features_list (list[str]): List of features to use.
            hyperparameters (dict[str, Any]): Hyperparameters for the model.
            wandb_run_name (str | None): Name of the run to be tracked on W&B.
            perform_cross_validation (bool): Whether to perform cross validation.

        Raises:
            ValueError: if run_mode is not one of "train" or "predict".
        """
        print("Sci-kit learn version: ", sklearn.__version__)  # noqa: T201
        if run_mode not in ["train", "predict"]:
            raise ValueError(
                f"run_mode must be one of 'train' or 'predict', got {run_mode}"
            )
        # Load common inputs
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        studies = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )
        v2g = V2G.from_parquet(session, variant_gene_path)
        # coloc = Colocalisation.from_parquet(self.session, self.colocalisation_path) # TODO: run step

        if run_mode == "train":
            # Process gold standard and L2G features
            study_locus_overlap = StudyLocusOverlap.from_parquet(
                session, study_locus_overlap_path
            )
            gs_curation = session.spark.read.json(gold_standard_curation_path)
            interactions = session.spark.read.parquet(gene_interactions_path)

            gold_standards = L2GGoldStandard.from_otg_curation(
                gold_standard_curation=gs_curation,
                v2g=v2g,
                study_locus_overlap=study_locus_overlap,
                interactions=interactions,
            )

            fm = L2GFeatureMatrix.generate_features(
                features_list=features_list,
                study_locus=credible_set,
                study_index=studies,
                variant_gene=v2g,
                # colocalisation=coloc,
            )

            # Join and fill null values with 0
            data = L2GFeatureMatrix(
                _df=fm.df.join(
                    f.broadcast(
                        gold_standards.df.drop("variantId", "studyId", "sources")
                    ),
                    on=["studyLocusId", "geneId"],
                    how="inner",
                ),
                _schema=L2GFeatureMatrix.get_schema(),
            ).fill_na()

            # Instantiate classifier
            estimator = SparkXGBClassifier(
                eval_metric="logloss",
                features_col="features",
                label_col="label",
                max_depth=5,
            )
            l2g_model = LocusToGeneModel(
                features_list=list(features_list), estimator=estimator
            )
            if perform_cross_validation:
                # Perform cross validation to extract what are the best hyperparameters
                cv_folds = hyperparameters.get("cross_validation_folds", 5)
                LocusToGeneTrainer.cross_validate(
                    l2g_model=l2g_model,
                    data=data,
                    num_folds=cv_folds,
                )
            else:
                # Train model
                LocusToGeneTrainer.train(
                    data=data,
                    l2g_model=l2g_model,
                    features_list=list(features_list),
                    model_path=model_path,
                    evaluate=True,
                    wandb_run_name=wandb_run_name,
                    **hyperparameters,
                )
                session.logger.info(model_path)

        if run_mode == "predict":
            if not model_path or not predictions_path:
                raise ValueError(
                    "model_path and predictions_path must be set for predict mode."
                )
            predictions = L2GPrediction.from_credible_set(
                model_path,
                features_list,
                credible_set,
                studies,
                v2g,
                # coloc
            )
            predictions.df.write.mode(session.write_mode).parquet(predictions_path)
            session.logger.info(predictions_path)
