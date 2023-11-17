"""L2G gold standard dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import get_record_with_maximum_value
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.dataset.study_locus_overlap import StudyLocusOverlap
    from otg.dataset.v2g import V2G


@dataclass
class L2GGoldStandard(Dataset):
    """L2G gold standard dataset."""

    INTERACTION_THRESHOLD = 0.7

    @classmethod
    def from_otg_curation(
        cls: type[L2GGoldStandard],
        gold_standard_curation: DataFrame,
        v2g: V2G,
        study_locus_overlap: StudyLocusOverlap,
        interactions: DataFrame,
    ) -> L2GGoldStandard:
        """Initialise L2GGoldStandard from source dataset.

        Args:
            gold_standard_curation (DataFrame): Gold standard curation dataframe, extracted from
            v2g (V2G): Variant to gene dataset to bring distance between a variant and a gene's TSS
            study_locus_overlap (StudyLocusOverlap): Study locus overlap dataset to remove duplicated loci
            interactions (DataFrame): Gene-gene interactions dataset to remove negative cases where the gene interacts with a positive gene

        Returns:
            L2GGoldStandard: L2G Gold Standard dataset
        """
        from otg.datasource.open_targets.l2g_gold_standard import (
            OpenTargetsL2GGoldStandard,
        )

        interactions_df = cls.process_gene_interactions(interactions)

        return (
            OpenTargetsL2GGoldStandard.as_l2g_gold_standard(gold_standard_curation, v2g)
            .filter_unique_associations(study_locus_overlap)
            .remove_false_negatives(interactions_df)
        )

    @classmethod
    def get_schema(cls: type[L2GGoldStandard]) -> StructType:
        """Provides the schema for the L2GGoldStandard dataset.

        Returns:
            StructType: Spark schema for the L2GGoldStandard dataset
        """
        return parse_spark_schema("l2g_gold_standard.json")

    @classmethod
    def process_gene_interactions(
        cls: Type[L2GGoldStandard], interactions: DataFrame
    ) -> DataFrame:
        """Extract top scoring gene-gene interaction from the interactions dataset of the Platform.

        Args:
            interactions (DataFrame): Gene-gene interactions dataset from the Open Targets Platform

        Returns:
            DataFrame: Top scoring gene-gene interaction per pair of genes

        Examples:
            >>> interactions = spark.createDataFrame([("gene1", "gene2", 0.8), ("gene1", "gene2", 0.5), ("gene2", "gene3", 0.7)], ["targetA", "targetB", "scoring"])
            >>> L2GGoldStandard.process_gene_interactions(interactions).show()
            +-------+-------+-----+
            |geneIdA|geneIdB|score|
            +-------+-------+-----+
            |  gene1|  gene2|  0.8|
            |  gene2|  gene3|  0.7|
            +-------+-------+-----+
            <BLANKLINE>
        """
        return get_record_with_maximum_value(
            interactions,
            ["targetA", "targetB"],
            "scoring",
        ).selectExpr(
            "targetA as geneIdA",
            "targetB as geneIdB",
            "scoring as score",
        )

    def filter_unique_associations(
        self: L2GGoldStandard,
        study_locus_overlap: StudyLocusOverlap,
    ) -> L2GGoldStandard:
        """Refines the gold standard to filter out loci that are not independent. redundant loci by testing they are truly independent.

        Args:
            study_locus_overlap (StudyLocusOverlap): A dataset detailing variants that overlap between StudyLocus.

        Returns:
            L2GGoldStandard: L2GGoldStandard updated to exclude false negatives and redundant positives.
        """
        # TODO: Test this logic
        self.df = (
            self.df.alias("left")
            .join(
                study_locus_overlap.df.select(
                    "leftStudyLocusId", "rightStudyLocusId"
                ).alias("right"),
                (f.col("left.variantId") == f.col("right.leftStudyLocusId"))
                | (f.col("left.variantId") == f.col("right.rightStudyLocusId")),
                how="left",
            )
            .distinct()
        )
        return self

    def remove_false_negatives(
        self: L2GGoldStandard,
        interactions_df: DataFrame,
    ) -> L2GGoldStandard:
        """Refines the gold standard to remove negative gold standard instances where the gene interacts with a positive gene.

        Args:
            interactions_df (DataFrame): Top scoring gene-gene interaction per pair of genes

        Returns:
            L2GGoldStandard: A refined set of locus-to-gene associations with increased reliability, having excluded loci that were likely false negatives due to gene-gene interaction confounding.
        """
        # TODO: Test this logic
        self.df = (
            self.df.alias("left")
            .join(
                interactions_df.alias("interactions"),
                (f.col("left.geneId") == f.col("interactions.geneIdA"))
                | (f.col("left.geneId") == f.col("interactions.geneIdB")),
                how="left",
            )
            .withColumn(
                "interacting",
                (f.col("score") > self.INTERACTION_THRESHOLD),
            )
            .filter(
                ~(
                    (
                        f.col("goldStandardSet") == 0
                    )  # TODO: goldStandardSet is a string, not an int
                    & (f.col("interacting"))
                    & (
                        (f.col("left.geneId") == f.col("interactions.geneIdA"))
                        | (f.col("left.geneId") == f.col("interactions.geneIdB"))
                    )
                )
            )
        )
        return self
