"""L2G gold standard dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import get_record_with_maximum_value
from otg.dataset.dataset import Dataset
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class L2GGoldStandard(Dataset):
    """L2G gold standard dataset.

    The curation is processed to generate a dataset with 2 labels:
    - Gold Standard Positive (GSP): Variant is within 500kb of gene
    - Gold Standard Negative (GSN): Variant is not within 500kb of gene
    """

    _schema: StructType = parse_spark_schema("l2g_feature.json")  # TODO: define schema

    @staticmethod
    def process_gene_interactions(etl: ETLSession, interactions_path: str) -> DataFrame:
        """Extract top scoring gene-gene interaction from the interactions dataset of the Platform."""
        return get_record_with_maximum_value(
            etl.spark.read.parquet(interactions_path),
            ["targetA", "targetB"],
            "scoring",
        ).selectExpr(
            "targetA as geneIdA",
            "targetB as geneIdB",
            "scoring as score",
        )

    @classmethod
    def from_curation(
        cls: Type[L2GGoldStandard],
        etl: ETLSession,
        gold_standard_curation: str,
        v2g_path: str,
        study_locus_path: str,
        study_locus_overlap_path: str,
        interactions_path: str,
    ) -> L2GGoldStandard:
        """Process gold standard curation to use as training data."""
        overlaps_df = StudyLocusOverlap.from_parquet(
            etl, study_locus_overlap_path
        ).df.select("left_studyLocusId", "right_studyLocusId")
        interactions_df = L2GGoldStandard.process_gene_interactions(
            etl, interactions_path
        )
        return cls(
            _df=etl.spark.read.json(gold_standard_curation)
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
            .filter(
                f.col("gold_standard_info.highest_confidence").isin(["High", "Medium"])
            )
            # Bring studyLocusId - TODO: what if I don't have one?
            .join(
                StudyLocus.from_parquet(etl, study_locus_path).df.select(
                    "studyId", "variantId", "studyLocusId"
                ),
                on=["studyId", "variantId"],
                how="inner",
            )
            # Assign Positive or Negative Status based on confidence
            .join(
                V2G.from_parquet(etl, v2g_path).df.select(
                    "variantId", "geneId", "distance"
                ),
                on=["variantId", "geneId"],
                how="inner",
            )
            .withColumn(
                "gsStatus",
                f.when(f.col("distance") <= 500_000, f.lit(1)).otherwise(f.lit(0)),
            )
            # Remove redundant loci by testing they are truly independent
            .alias("left")
            .join(
                overlaps_df.alias("right"),
                (f.col("left.variantId") == f.col("right.left_studyLocusId"))
                | (f.col("left.variantId") == f.col("right.right_studyLocusId")),
                how="left",
            )
            .distinct()
            # Remove redundant genes by testing they do not interact with a GSP gene
            .join(
                interactions_df.alias("interactions"),
                (f.col("left.geneId") == f.col("interactions.geneIdA"))
                | (f.col("left.geneId") == f.col("interactions.geneIdB")),
                how="left",
            )
            .withColumn("interacting", (f.col("score") > 0.7))
            # filter out genes where geneIdA has gsStatus Negative but geneIdA and gene IdB are interacting
            .filter(
                ~(
                    (f.col("gsStatus") == 0)
                    & (f.col("interacting"))
                    & (
                        (f.col("left.geneId") == f.col("interactions.geneIdA"))
                        | (f.col("left.geneId") == f.col("interactions.geneIdB"))
                    )
                )
            )
        )
