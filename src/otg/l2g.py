"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omegaconf import DictConfig

from otg.config.l2g import LocusToGeneConfig
from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.method.locus_to_gene import LocusToGeneTrainer

if TYPE_CHECKING:
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
            gold_standards = L2GGoldStandard.from_curation(
                etl=self.session,
                study_locus_path=self.study_locus_path,
                v2g_path=self.variant_gene_path,
                study_locus_overlap_path=self.study_locus_overlap_path,
                gold_standard_curation=self.gold_standard_curation_path,
                interactions_path=self.gene_interactions_path,
            )
            print(type(gold_standards))
            #     gold_standards = self.etl.spark.read.parquet(
            #         "/Users/irenelopez/MEGAsync/EBI/repos/genetics_etl_python/mock_data/processed_gs"
            #     )
            # fm = L2GFeatureMatrix.generate_features(
            #     etl=self.session,
            #     study_locus_path=self.study_locus_path,
            #     study_index_path=self.study_index_path,
            #     variant_gene_path=self.variant_gene_path,
            #     colocalisation_path=self.colocalisation_path,
            # )
            fm = self.session.spark.read.parquet(self.feature_matrix_path)
            train, test = L2GFeatureMatrix(
                _df=gold_standards._df.join(fm, on="studyLocusId", how="inner")
            ).train_test_split(fraction=0.8)
            # TODO: data normalization and standardisation of features

        LocusToGeneTrainer.train(
            train_set=train,
            test_set=test,
            track=self.track,
            **self.hyperparameters,
            # TODO: Add push to hub
        )
