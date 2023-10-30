"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from dataclasses import dataclass, field

from omegaconf import MISSING
from xgboost.spark import SparkXGBClassifier

from otg.common.session import Session

# from otg.dataset.colocalisation import Colocalisation
from otg.dataset.l2g.feature import L2GFeatureMatrix
from otg.dataset.l2g.gold_standard import L2GGoldStandard
from otg.dataset.l2g.predictions import L2GPrediction
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G
from otg.method.locus_to_gene import LocusToGeneModel, LocusToGeneTrainer


@dataclass
class LocusToGeneStep:
    """Locus to gene step."""

    session: Session = Session()
    extended_spark_conf: dict[str, str] | None = None

    run_mode: str = MISSING
    wandb_run_name: str | None = None
    perform_cross_validation: bool = False
    model_path: str | None = None
    predictions_path: str | None = None
    study_locus_path: str = MISSING
    variant_gene_path: str = MISSING
    colocalisation_path: str = MISSING
    study_index_path: str = MISSING
    study_locus_overlap_path: str | None = None
    gold_standard_curation_path: str | None = None
    gene_interactions_path: str | None = None
    features_list: list[str] = field(
        default_factory=lambda: [
            # average distance of all tagging variants to gene TSS
            "distanceTssMean",
            # # minimum distance of all tagging variants to gene TSS
            # "distanceTssMinimum",
            # # max clpp for each (study, locus, gene) aggregating over all eQTLs
            # "eqtlColocClppLocalMaximum",
            # # max clpp for each (study, locus) aggregating over all eQTLs
            # "eqtlColocClppNeighborhoodMaximum",
            # # max log-likelihood ratio value for each (study, locus, gene) aggregating over all eQTLs
            # "eqtlColocLlrLocalMaximum",
            # # max log-likelihood ratio value for each (study, locus) aggregating over all eQTLs
            # "eqtlColocLlrNeighborhoodMaximum",
            # # max clpp for each (study, locus, gene) aggregating over all pQTLs
            # "pqtlColocClppLocalMaximum",
            # # max clpp for each (study, locus) aggregating over all pQTLs
            # "pqtlColocClppNeighborhoodMaximum",
            # # max log-likelihood ratio value for each (study, locus, gene) aggregating over all pQTLs
            # "pqtlColocLlrLocalMaximum",
            # # max log-likelihood ratio value for each (study, locus) aggregating over all pQTLs
            # "pqtlColocLlrNeighborhoodMaximum",
            # # max clpp for each (study, locus, gene) aggregating over all sQTLs
            # "sqtlColocClppLocalMaximum",
            # # max clpp for each (study, locus) aggregating over all sQTLs
            # "sqtlColocClppNeighborhoodMaximum",
            # # max log-likelihood ratio value for each (study, locus, gene) aggregating over all sQTLs
            # "sqtlColocLlrLocalMaximum",
            # # max log-likelihood ratio value for each (study, locus) aggregating over all sQTLs
            # "sqtlColocLlrNeighborhoodMaximum",
        ]
    )
    hyperparameters: dict = field(
        default_factory=lambda: {
            "max_depth": 5,
            "loss_function": "binary:logistic",
        }
    )

    def __post_init__(self: LocusToGeneStep) -> None:
        """Run step."""
        # Load common inputs
        study_locus = StudyLocus.from_parquet(
            self.session, self.study_locus_path, recursiveFileLookup=True
        )
        studies = StudyIndex.from_parquet(self.session, self.study_index_path)
        v2g = V2G.from_parquet(self.session, self.variant_gene_path)
        # coloc = Colocalisation.from_parquet(self.session, self.colocalisation_path) # TODO: run step

        if self.run_mode == "train":
            # Process gold standard and L2G features
            study_locus_overlap = StudyLocusOverlap.from_parquet(
                self.session, self.study_locus_overlap_path
            )
            gs_curation = self.session.spark.read.json(self.gold_standard_curation_path)
            interactions = self.session.spark.read.parquet(self.gene_interactions_path)

            gold_standards = L2GGoldStandard.from_otg_curation(
                gold_standard_curation=gs_curation,
                v2g=v2g,
                study_locus_overlap=study_locus_overlap,
                interactions=interactions,
            )

            fm = L2GFeatureMatrix.generate_features(
                study_locus=study_locus,
                study_index=studies,
                variant_gene=v2g,
                # colocalisation=coloc,
            )

            # Join and fill null values with 0
            data = L2GFeatureMatrix(
                _df=gold_standards.df.drop("sources").join(
                    fm.df, on=["studyLocusId", "geneId"], how="inner"
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
                features_list=list(self.features_list), estimator=estimator
            )
            if self.perform_cross_validation:
                # Perform cross validation to extract what are the best hyperparameters
                cv_folds = self.hyperparameters.get("cross_validation_folds", 5)
                LocusToGeneTrainer.cross_validate(
                    l2g_model=l2g_model,
                    data=data,
                    num_folds=cv_folds,
                )
            else:
                # Train model
                model = LocusToGeneTrainer.train(
                    data=data,
                    l2g_model=l2g_model,
                    features_list=list(self.features_list),
                    model_path=self.model_path,
                    evaluate=True,
                    wandb_run_name=self.wandb_run_name,
                    **self.hyperparameters,
                )
                model.save(self.model_path)
                self.session.logger.info(
                    f"Finished L2G step. L2G model saved to {self.model_path}"
                )

        if self.run_mode == "predict":
            if not self.model_path or not self.predictions_path:
                raise ValueError(
                    "model_path and predictions_path must be set for predict mode."
                )
            predictions = L2GPrediction.from_study_locus(
                self.model_path,
                study_locus,
                studies,
                v2g,
                # coloc
            )
            predictions.df.write.mode(self.session.write_mode).parquet(
                self.predictions_path
            )
            self.session.logger.info(
                f"Finished L2G step. L2G predictions saved to {self.predictions_path}"
            )
