"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from dataclasses import dataclass

from otg.config import LocusToGeneConfig

# FIXME: reason why this is not working is that there is a default etl definition before than a positional param
# from otg.common.spark_helpers import get_record_with_maximum_value
# from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
# from otg.dataset.study_locus import StudyLocus
# from otg.dataset.study_locus_overlap import StudyLocusOverlap
# from otg.dataset.v2g import V2G
# from otg.method.locus_to_gene import LocusToGeneTrainer


@dataclass
class LocusToGeneStep(LocusToGeneConfig):
    """Locus to gene step."""

    def run(self: LocusToGeneStep) -> None:
        """Run Locus to Gene step."""
        # self.etl.logger.info(f"Executing {self.id} step")

        print("Running Locus to Gene step")

        # if self.cfg.run_mode == "train":
        #     gold_standards = get_gold_standards(
        #         etl=self.etl,
        #         study_locus_path=self.cfg.study_locus_path,
        #         v2g_path=self.cfg.variant_gene_path,
        #         study_locus_overlap_path=self.cfg.study_locus_overlap_path,
        #         gold_standard_curation=self.cfg.gold_standard_curation_path,
        #         interactions_path=self.cfg.gene_interactions_path,
        #     )
        #     gold_standards = self.etl.spark.read.parquet(
        #         "/Users/irenelopez/MEGAsync/EBI/repos/genetics_etl_python/mock_data/processed_gs"
        #     )
        #     fm = L2GFeatureMatrix  # FIXME: debug credset TODO: inverse matrix
        #     data = gold_standards.join(
        #         fm, on="studyLocusId", how="inner"
        #     ).train_test_split(frac=0.1, seed=42)
        #     # TODO: data normalization and standardisation of features

        #     LocusToGeneTrainer.train(
        #         train_set=data["train"],
        #         test_set=data["test"],
        #         **self.hyperparameters,
        #         # TODO: Add push to hub, and push to W&B
        #     )


# def get_gold_standards(
#     etl: ETLSession,
#     gold_standard_curation: str,
#     v2g_path: str,
#     study_locus_path: str,
#     study_locus_overlap_path: str,
#     interactions_path: str,
# ) -> DataFrame:
#     """Process gold standard curation to use as training data."""
#     # FIXME: assign function to class - something is wrong instantiating the classes, used to work
#     overlaps_df = StudyLocusOverlap.from_parquet(
#         etl, study_locus_overlap_path
#     ).df.select("left_studyLocusId", "right_studyLocusId")
#     interactions_df = process_gene_interactions(etl, interactions_path)
#     return (
#         etl.spark.read.json(gold_standard_curation)
#         .select(
#             f.col("association_info.otg_id").alias("studyId"),
#             f.col("gold_standard_info.gene_id").alias("geneId"),
#             f.concat_ws(
#                 "_",
#                 f.col("sentinel_variant.locus_GRCh38.chromosome"),
#                 f.col("sentinel_variant.locus_GRCh38.position"),
#                 f.col("sentinel_variant.alleles.reference"),
#                 f.col("sentinel_variant.alleles.alternative"),
#             ).alias("variantId"),
#         )
#         .filter(f.col("gold_standard_info.highest_confidence").isin(["High", "Medium"]))
#         # Bring studyLocusId - TODO: what if I don't have one?
#         .join(
#             StudyLocus.from_parquet(etl, study_locus_path).df.select(
#                 "studyId", "variantId", "studyLocusId"
#             ),
#             on=["studyId", "variantId"],
#             how="inner",
#         )
#         # Assign Positive or Negative Status based on confidence
#         .join(
#             V2G.from_parquet(etl, v2g_path).df.select(
#                 "variantId", "geneId", "distance"
#             ),
#             on=["variantId", "geneId"],
#             how="inner",
#         )
#         .withColumn(
#             "gsStatus",
#             f.when(f.col("distance") <= 500_000, f.lit(1)).otherwise(f.lit(0)),
#         )
#         # Remove redundant loci
#         .alias("left")
#         .join(
#             overlaps_df.alias("right"),
#             (f.col("left.variantId") == f.col("right.left_studyLocusId"))
#             | (f.col("left.variantId") == f.col("right.right_studyLocusId")),
#             how="left",
#         )
#         .distinct()
#         # Remove redundant genes
#         .join(
#             interactions_df.alias("interactions"),
#             (f.col("left.geneId") == f.col("interactions.geneIdA"))
#             | (f.col("left.geneId") == f.col("interactions.geneIdB")),
#             how="left",
#         )
#         .withColumn("interacting", (f.col("score") > 0.7))
#         # filter out genes where geneIdA has gsStatus Negative but geneIdA and gene IdB are interacting
#         .filter(
#             ~(
#                 (f.col("gsStatus") == 0)
#                 & (f.col("interacting"))
#                 & (
#                     (f.col("left.geneId") == f.col("interactions.geneIdA"))
#                     | (f.col("left.geneId") == f.col("interactions.geneIdB"))
#                 )
#             )
#         )
#     )


# def process_gene_interactions(etl: ETLSession, interactions_path: str) -> DataFrame:
#     """Extract top scoring gene-gene interaction from the Platform."""
#     # FIXME: assign function to class
#     return get_record_with_maximum_value(
#         etl.spark.read.parquet(interactions_path),
#         ["targetA", "targetB"],
#         "scoring",
#     ).selectExpr(
#         "targetA as geneIdA",
#         "targetB as geneIdB",
#         "scoring as score",
#     )
