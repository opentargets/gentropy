"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from xgboost.spark import SparkXGBClassifier

from otg.common.spark_helpers import _convert_from_long_to_wide
from otg.config import LocusToGeneConfig
from otg.dataset.l2g.feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g.gold_standard import L2GGoldStandard
from otg.dataset.l2g.predictions import L2GPredictions
from otg.method.locus_to_gene import LocusToGeneModel, LocusToGeneTrainer

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from otg.common.session import Session


@dataclass
class LocusToGeneStep(LocusToGeneConfig):
    """Locus to gene step."""

    etl: Session
    run_mode: str
    study_locus: DictConfig
    v2g: DictConfig
    colocalisation: DictConfig
    study_index: DictConfig
    study_locus_overlap: DictConfig
    l2g_curation: DictConfig
    gene_interactions: DictConfig
    hyperparameters: DictConfig
    l2g_model: DictConfig | None = None
    id: str = "locus_to_gene"

    def run(self: LocusToGeneStep) -> None:
        """Run Locus to Gene step."""
        self.session.logger.info(f"Executing {self.id} step")

        print("Config for the L2G step: ", self)

        if self.run_mode == "train":
            # Process gold standard and L2G features

            # gold_standards = L2GGoldStandard.from_curation(
            #     session=self.session,
            #     study_locus_path=self.study_locus_path,
            #     v2g_path=self.variant_gene_path,
            #     study_locus_overlap_path=self.study_locus_overlap_path,
            #     gold_standard_curation=self.gold_standard_curation_path,
            #     interactions_path=self.gene_interactions_path,
            # )

            # fm = L2GFeatureMatrix.generate_features(
            #     session=self.session,
            #     study_locus_path=self.study_locus_path,
            #     study_index_path=self.study_index_path,
            #     variant_gene_path=self.variant_gene_path,
            #     colocalisation_path=self.colocalisation_path,
            # )

            gold_standards = L2GGoldStandard(
                _df=self.session.spark.read.parquet(self.gold_standard_processed_path)
            )
            fm = _convert_from_long_to_wide(
                self.session.spark.read.parquet(self.feature_matrix_path),
                id_vars=["studyLocusId", "geneId"],
                var_name="feature",
                value_name="value",
            )

            # Join and fill null values with 0
            data = L2GFeatureMatrix(
                _df=gold_standards._df.join(
                    fm, on=["studyLocusId", "geneId"], how="inner"
                ).transform(L2GFeatureMatrix.fill_na),
            )

            # Instantiate classifier
            estimator = SparkXGBClassifier(
                eval_metric="logloss",
                features_col="features",
                label_col="label",
                max_depth=5,
            )
            l2g_model = LocusToGeneModel(
                features_list=list(self.features_list), estimator=estimator
            )
            if self.hyperparameters.cross_validation_folds:
                # Perform cross validation to extract what are the best hyperparameters
                LocusToGeneTrainer.cross_validate(
                    l2g_model=l2g_model,
                    data=data,
                    num_folds=self.hyperparameters.cross_validation_folds,
                )
                # self.wandb_run_name = f"{self.wandb_run_name}_cv_best_params"
            else:
                # Train model
                LocusToGeneTrainer.train(
                    data=data,
                    l2g_model=l2g_model,
                    feature_cols=list(self.features_list),
                    model_path=self.model_path,
                    wandb_run_name=self.wandb_run_name,
                    **self.hyperparameters,
                )

        if self.run_mode == "predict":
            predictions = L2GPredictions.from_study_locus(
                self.session, self.feature_matrix_path, self.model_path
            )
            predictions.df.write.mode(self.session.write_mode).parquet(
                self.predictions_path
            )

            self.session.logger.info(
                f"Finished {self.id} step. L2G predictions saved to {self.predictions_path}"
            )
