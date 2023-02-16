"""Step to run Locus to Gene either for inference or for training."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra
import pyspark.sql.functions as f

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from pyspark.sql import DataFrame

from otg.common.session import ETLSession
from otg.common.spark_helpers import get_record_with_maximum_value
from otg.dataset.l2g_feature_matrix import L2G, L2GFeatureMatrix
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G
from otg.method.locus_to_gene import L2GTrainer


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run Locus to Gene."""
    etl = ETLSession(cfg)

    if cfg.run_mode == "train":
        gold_standards = get_gold_standards(
            etl,
            cfg.gold_standard.curation,
            cfg.feature_inputs.v2g,
            cfg.feature_inputs.study_locus,
            cfg.feature_inputs.study_locus_overlap,
            cfg.gold_standard.interactions,
        )
        fm = L2GFeatureMatrix  # TODO: inverse matrix
        data = gold_standards.join(fm, on="studyLocusId", how="inner").train_test_split(
            frac=0.1, seed=42
        )
        # TODO: data normalization and standardisation of features

        L2GTrainer.train(
            train_set=data["train"],
            test_set=data["test"],
            **cfg.hyperparameters,
            # TODO: Add push to hub, and push to W&B
        )


def get_gold_standards(
    etl: ETLSession,
    gold_standard_curation: str,
    v2g_path: str,
    study_locus_path: str,
    study_locus_overlap_path: str,
    interactions_path: str,
) -> L2G:
    """Process gold standard curation to use as training data."""
    # FIXME: assign function to class
    overlaps_df = StudyLocusOverlap.from_parquet(study_locus_overlap_path).select(
        "left_studyLocusId", "right_studyLocusId"
    )
    interactions = process_gene_interactions(etl, interactions_path)
    return (
        etl.spark.read.parquet(gold_standard_curation)
        .select(
            f.col("association_info.otg_id").alias("studyId"),
            f.col("gold_standard_info.gene_id").alias("geneId"),
            f.concat_ws(
                "_",
                f.col("sentinel_variant.locus_GRCh38.chromosome"),
                f.col("sentinel_variant.locus_GRCh38.position"),
                f.col("sentinel_variant.alleles.reference"),
                f.col("sentinel_variant.alleles.alternative"),
            ).alias("variantId"),
        )
        .filter(f.col("gold_standard_info.highest_confidence").isin(["High", "Medium"]))
        # Bring studyLocusId - TODO: what if I don't have one?
        .join(
            StudyLocus.from_parquet(study_locus_path).select(
                "studyId", "variantId", "studyLocusId"
            ),
            on=["studyId", "variantId"],
            how="inner",
        )
        # Assign Positive or Negative Status based on confidence
        .join(
            V2G.from_parquet(v2g_path).select("variantId", "geneId", "distance"),
            on=["variantId", "geneId"],
            how="inner",
        )
        .withColumn(
            "gsStatus",
            f.when(f.col("distance") <= 500_000, "Positive").otherwise("Negative"),
        )
        # Remove redundant loci
        .alias("left")
        .join(
            overlaps_df.alias("right"),
            (f.col("left.variantId") == f.col("right.left_studyLocusId"))
            | (f.col("left.variantId") == f.col("right.right_studyLocusId")),
            how="left",
        )
        .distinct()
        # Remove redundant genes
        .join(
            interactions.alias("interactions"),
            (f.col("left.geneId") == f.col("interactions.geneIdA"))
            | (f.col("left.geneId") == f.col("interactions.geneIdB")),
            how="left",
        )
        .withColumn("interacting", (f.col("scoring") > 0.7))
        # filter out genes where geneIdA has gsStatus Negative but geneIdA and gene IdB are interacting
        .filter(
            ~(
                (f.col("gsStatus") == "Negative")
                & (f.col("interacting"))
                & (
                    (f.col("left.geneId") == f.col("interactions.geneIdA"))
                    | (f.col("left.geneId") == f.col("interactions.geneIdB"))
                )
            )
        )
    )


def process_gene_interactions(etl: ETLSession, interactions_path: str) -> DataFrame:
    """Extract top scoring gene-gene interaction from the Platform."""
    # FIXME: assign function to class
    return get_record_with_maximum_value(
        etl.spark.read.parquet(interactions_path),
        ["targetA", "targetB"],
        "scoring",
    ).selectExpr(
        "targetA as geneIdA",
        "targetB as geneIdB",
        "scoring as score",
    )
