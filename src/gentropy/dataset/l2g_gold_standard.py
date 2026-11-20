"""L2G gold standard dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark import get_record_with_maximum_value
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.credible_set import CredibleSet
    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
    from gentropy.dataset.locus_overlap import LocusOverlap
    from gentropy.dataset.variant_index import VariantIndex


@dataclass
class L2GGoldStandard(Dataset):
    """L2G gold standard dataset."""

    INTERACTION_THRESHOLD = 0.7
    GS_POSITIVE_LABEL = "positive"
    GS_NEGATIVE_LABEL = "negative"

    @classmethod
    def from_otg_curation(
        cls: type[L2GGoldStandard],
        gold_standard_curation: DataFrame,
        credible_set: CredibleSet,
        study_locus_overlap: LocusOverlap,
        variant_index: VariantIndex,
        interactions: DataFrame,
    ) -> L2GGoldStandard:
        """Initialise L2GGoldStandard from source dataset.

        Args:
            gold_standard_curation (DataFrame): Gold standard curation dataframe, extracted from
            credible_set (CredibleSet): Credible sets led by a curated sentinel, which give the curated loci their `credibleSetId`
            study_locus_overlap (LocusOverlap): Overlaps between those credible sets, to remove duplicated loci
            variant_index (VariantIndex): Dataset to bring distance between a variant and a gene's footprint
            interactions (DataFrame): Gene-gene interactions dataset to remove negative cases where the gene interacts with a positive gene

        Returns:
            L2GGoldStandard: L2G Gold Standard dataset
        """
        from gentropy.datasource.open_targets.l2g_gold_standard import (
            OpenTargetsL2GGoldStandard,
        )

        interactions_df = cls.process_gene_interactions(interactions)

        return (
            OpenTargetsL2GGoldStandard.as_l2g_gold_standard(
                gold_standard_curation, credible_set, variant_index
            )
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
        cls: type[L2GGoldStandard], interactions: DataFrame
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

    def build_feature_matrix(
        self: L2GGoldStandard,
        full_feature_matrix: L2GFeatureMatrix,
        credible_set: CredibleSet,
    ) -> L2GFeatureMatrix:
        """Return a feature matrix for study loci in the gold standard.

        Args:
            full_feature_matrix (L2GFeatureMatrix): Feature matrix for all study loci to join on
            credible_set (CredibleSet): Full credible sets to annotate the feature matrix with lead variant and study IDs and perform the join

        Returns:
            L2GFeatureMatrix: Feature matrix for study loci in the gold standard
        """
        from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix

        return L2GFeatureMatrix(
            _df=full_feature_matrix._df.join(
                credible_set.df.select(
                    f.col("id").alias("credibleSetId"),
                    f.col("leadVariantId").alias("variantId"),
                    "studyId",
                ),
                "credibleSetId",
                "left",
            )
            .join(
                f.broadcast(self.df.drop("credibleSetId", "sources")),
                on=["studyId", "variantId", "geneId"],
                how="inner",
            )
            .filter(f.col("isProteinCoding") == 1)
            .drop("studyId", "variantId")
            .distinct(),
            with_gold_standard=True,
        ).fill_na()

    def filter_unique_associations(
        self: L2GGoldStandard,
        study_locus_overlap: LocusOverlap,
    ) -> L2GGoldStandard:
        """Refines the gold standard to filter out loci that are not independent.

        Two loci overlap when they share a variant. For each pair of overlapping loci pointing to
        the same gene:
        - If one is positive and the other negative, we keep the positive one.
        - If both are positive or both negative, we keep the one with the smaller `credibleSetId`.
        Loci pointing to different genes are kept, overlapping or not.

        The gold standard and the overlaps are both keyed by `CredibleSet.id`. Before, the curated
        loci were keyed by a hash of study and sentinel that no credible set carried, so no overlap
        ever matched and this filter removed nothing.

        Args:
            study_locus_overlap (LocusOverlap): A dataset detailing variants that overlap between StudyLocus.

        Returns:
            L2GGoldStandard: L2GGoldStandard updated to exclude false negatives and redundant positives.
        """
        pairs = (
            study_locus_overlap._convert_to_square_matrix()
            .df.select("leftLocusId", "rightLocusId")
            .distinct()
        )
        labels = self.df.select("credibleSetId", "geneId", "goldStandardSet")
        left, right = labels.alias("left"), labels.alias("right")
        # (locus, gene) rows explained by an overlapping locus that wins the pair.
        dominated = (
            left.join(pairs, f.col("left.credibleSetId") == f.col("leftLocusId"))
            .join(
                right,
                (f.col("rightLocusId") == f.col("right.credibleSetId"))
                & (f.col("left.geneId") == f.col("right.geneId")),
            )
            .filter(
                (
                    (f.col("right.goldStandardSet") == self.GS_POSITIVE_LABEL)
                    & (f.col("left.goldStandardSet") == self.GS_NEGATIVE_LABEL)
                )
                | (
                    (f.col("right.goldStandardSet") == f.col("left.goldStandardSet"))
                    & (f.col("right.credibleSetId") < f.col("left.credibleSetId"))
                )
            )
            .select("left.credibleSetId", "left.geneId")
            .distinct()
        )
        unique_associations = self.df.join(
            dominated, on=["credibleSetId", "geneId"], how="left_anti"
        ).select(*self.df.columns)
        return L2GGoldStandard(_df=unique_associations, _schema=self.get_schema())

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
        squared_interactions = interactions_df.unionByName(
            interactions_df.selectExpr(
                "geneIdB as geneIdA", "geneIdA as geneIdB", "score"
            )
        ).filter(f.col("score") > self.INTERACTION_THRESHOLD)
        df = (
            self.df.alias("left")
            .join(
                # bring gene partners
                squared_interactions.alias("right"),
                f.col("left.geneId") == f.col("right.geneIdA"),
                "left",
            )
            .withColumnRenamed("geneIdB", "interactorGeneId")
            .join(
                # bring gold standard status for gene partners
                self.df.selectExpr(
                    "geneId as interactorGeneId",
                    "goldStandardSet as interactorGeneIdGoldStandardSet",
                ),
                "interactorGeneId",
                "left",
            )
            # remove self-interactions
            .filter(
                (f.col("geneId") != f.col("interactorGeneId"))
                | (f.col("interactorGeneId").isNull())
            )
            # remove false negatives
            .filter(
                # drop rows where the GS gene is negative but the interactor is a GS positive
                ~(f.col("goldStandardSet") == "negative")
                & (f.col("interactorGeneIdGoldStandardSet") == "positive")
                |
                # keep rows where the gene does not interact
                (f.col("interactorGeneId").isNull())
            )
            .select(*self.df.columns)
            .distinct()
        )
        return L2GGoldStandard(_df=df, _schema=self.get_schema())
