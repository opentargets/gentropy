"""L2G gold standard dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import get_record_with_maximum_value
from otg.dataset.dataset import Dataset
from otg.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.dataset.study_locus_overlap import StudyLocusOverlap
    from otg.dataset.v2g import V2G


@dataclass
class L2GGoldStandard(Dataset):
    """L2G gold standard dataset.

    The curation is processed to generate a dataset with 2 labels:
    - Gold Standard Positive (GSP): Variant is within 500kb of gene
    - Gold Standard Negative (GSN): Variant is not within 500kb of gene
    """

    @staticmethod
    def process_gene_interactions(interactions: DataFrame) -> DataFrame:
        """Extract top scoring gene-gene interaction from the interactions dataset of the Platform."""
        return get_record_with_maximum_value(
            interactions,
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
        gold_standard_curation: DataFrame,
        v2g: V2G,
        study_locus_overlap: StudyLocusOverlap,
        interactions: DataFrame,
    ) -> L2GGoldStandard:
        """Process gold standard curation to use as training data."""
        overlaps_df = study_locus_overlap._df.select(
            "leftStudyLocusId", "rightStudyLocusId"
        )
        interactions_df = L2GGoldStandard.process_gene_interactions(interactions)
        return cls(
            _df=(
                gold_standard_curation.select(
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
                .withColumn(
                    "studyLocusId",
                    StudyLocus.assign_study_locus_id("studyId", "variantId"),
                )
                .filter(
                    f.col("gold_standard_info.highest_confidence").isin(
                        ["High", "Medium"]
                    )
                )
                # Assign Positive or Negative Status based on confidence
                .join(
                    v2g.df.select("variantId", "geneId", "distance"),
                    on=["variantId", "geneId"],
                    how="inner",
                )
                .withColumn(
                    "goldStandardSet",
                    f.when(f.col("distance") <= 500_000, f.lit("positive")).otherwise(
                        f.lit("negative")
                    ),
                )
                # Remove redundant loci by testing they are truly independent
                .alias("left")
                .join(
                    overlaps_df.alias("right"),
                    (f.col("left.variantId") == f.col("right.leftStudyLocusId"))
                    | (f.col("left.variantId") == f.col("right.rightStudyLocusId")),
                    how="left",
                )
                .distinct()
                # Remove redundant genes by testing they do not interact with a positive gene
                .join(
                    interactions_df.alias("interactions"),
                    (f.col("left.geneId") == f.col("interactions.geneIdA"))
                    | (f.col("left.geneId") == f.col("interactions.geneIdB")),
                    how="left",
                )
                .withColumn("interacting", (f.col("score") > 0.7))
                # filter out genes where geneIdA has goldStandardSet negative but geneIdA and gene IdB are interacting
                .filter(
                    ~(
                        (f.col("goldStandardSet") == 0)
                        & (f.col("interacting"))
                        & (
                            (f.col("left.geneId") == f.col("interactions.geneIdA"))
                            | (f.col("left.geneId") == f.col("interactions.geneIdB"))
                        )
                    )
                )
                .select("studyLocusId", "geneId", "goldStandardSet")
                # TODO: comment from Daniel: include source of GS
            ),
            _schema=cls.get_schema(),
        )

    @classmethod
    def get_schema(cls: type[L2GGoldStandard]) -> StructType:
        """Provides the schema for the L2GGoldStandard dataset."""
        return parse_spark_schema("l2g_gold_standard.json")
