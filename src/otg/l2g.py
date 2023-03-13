"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from dataclasses import dataclass

from otg.config import LocusToGeneConfig
from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.method.locus_to_gene import LocusToGeneTrainer


@dataclass
class LocusToGeneStep(LocusToGeneConfig):
    """Locus to gene step."""

    def run(self: LocusToGeneStep) -> None:
        """Run Locus to Gene step."""
        self.etl.logger.info(f"Executing {self.id} step")

        print("Config for the L2G step: ", self)

        if self.cfg.run_mode == "train":
            gold_standards = L2GGoldStandard.get_gold_standards(
                etl=self.etl,
                study_locus_path=self.cfg.study_locus_path,
                v2g_path=self.cfg.variant_gene_path,
                study_locus_overlap_path=self.cfg.study_locus_overlap_path,
                gold_standard_curation=self.cfg.gold_standard_curation_path,
                interactions_path=self.cfg.gene_interactions_path,
            )
            #     gold_standards = self.etl.spark.read.parquet(
            #         "/Users/irenelopez/MEGAsync/EBI/repos/genetics_etl_python/mock_data/processed_gs"
            #     )
            fm = L2GFeatureMatrix  # FIXME: debug credset
            data = gold_standards.join(
                fm, on="studyLocusId", how="inner"
            ).train_test_split(frac=0.1, seed=42)
            # TODO: data normalization and standardisation of features

        LocusToGeneTrainer.train(
            train_set=data["train"],
            test_set=data["test"],
            **self.hyperparameters,
            # TODO: Add push to hub, and push to W&B
        )
