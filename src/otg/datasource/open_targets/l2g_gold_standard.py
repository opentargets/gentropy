"""Parser for OTPlatform locus to gene gold standards curation."""
from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from otg.common.spark_helpers import get_record_with_maximum_value
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G


class OpenTargetsL2GGoldStandard:
    """Parser for OTGenetics locus to gene gold standards curation.

    The curation is processed to generate a dataset with 2 labels:
        - Gold Standard Positive (GSP): When the lead variant is part of a curated list of GWAS loci with known gene-trait associations.
        - Gold Standard Negative (GSN): When the lead variant is not part of a curated list of GWAS loci with known gene-trait associations but is in the vicinity of a gene's TSS.
    """

    LOCUS_TO_GENE_WINDOW = 500_000
    GS_POSITIVE_LABEL = "positive"
    GS_NEGATIVE_LABEL = "negative"
    INTERACTION_THRESHOLD = 0.7

    @staticmethod
    def process_gene_interactions(interactions: DataFrame) -> DataFrame:
        """Extract top scoring gene-gene interaction from the interactions dataset of the Platform.

        Args:
            interactions (DataFrame): Gene-gene interactions dataset

        Returns:
            DataFrame: Top scoring gene-gene interaction per pair of genes
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

    @staticmethod
    def create_positive_set(gold_standard_curation: DataFrame) -> DataFrame:
        """Parse positive set from gold standard curation.

        Args:
            gold_standard_curation (DataFrame): Gold standard curation dataframe

        Returns:
            DataFrame: Positive set
        """
        return (
            gold_standard_curation.filter(
                f.col("gold_standard_info.highest_confidence").isin(["High", "Medium"])
            )
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
                f.col("metadata.set_label").alias("source"),
            )
            .withColumn(
                "studyLocusId",
                StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
            )
            .groupBy("studyLocusId", "studyId", "variantId", "geneId")
            .agg(f.collect_set("source").alias("sources"))
        )

    @staticmethod
    def create_full_set(positive_set: DataFrame, v2g: V2G) -> DataFrame:
        """Create full set of positive and negative evidence of locus to gene associations.

        Args:
            positive_set (DataFrame): Positive set
            v2g (V2G): Variant to gene dataset to bring distance between a variant and a gene's TSS

        Returns:
            DataFrame: Full set of positive and negative evidence of locus to gene associations
        """
        return positive_set.join(
            v2g.df.filter(
                f.col("distance") <= OpenTargetsL2GGoldStandard.LOCUS_TO_GENE_WINDOW
            ),
            on="variantId",
            how="left",
        ).withColumn(
            "goldStandardSet",
            f.when(
                (f.col("positives.geneId") == f.col("negatives.geneId"))
                # to keep the positives that are outside the v2g dataset
                | (f.col("negatives.geneId").isNull()),
                f.lit(OpenTargetsL2GGoldStandard.GS_POSITIVE_LABEL),
            ).otherwise(OpenTargetsL2GGoldStandard.GS_NEGATIVE_LABEL),
        )

    @staticmethod
    def remove_false_negatives(
        full_set: DataFrame, interactions_df: DataFrame
    ) -> DataFrame:
        """Remove redundant loci by testing they are truly independent.

        Args:
            full_set (DataFrame): Full set of positive and negative evidence of locus to gene associations. These include false negatives.
            interactions_df (DataFrame): Top scoring gene-gene interaction per pair of genes

        Returns:
            DataFrame: Full set of positive and negative evidence of locus to gene associations. False negatives are removed.
        """
        return (
            full_set.alias("left")
            .join(
                interactions_df.alias("interactions"),
                (f.col("left.geneId") == f.col("interactions.geneIdA"))
                | (f.col("left.geneId") == f.col("interactions.geneIdB")),
                how="left",
            )
            .withColumn(
                "interacting",
                (f.col("score") > OpenTargetsL2GGoldStandard.INTERACTION_THRESHOLD),
            )
            .filter(
                ~(
                    (
                        f.col("goldStandardSet") == 0
                    )  # bugfix: goldStandardSet is a string, not an int
                    & (f.col("interacting"))
                    & (
                        (f.col("left.geneId") == f.col("interactions.geneIdA"))
                        | (f.col("left.geneId") == f.col("interactions.geneIdB"))
                    )
                )
            )
        )

    @staticmethod
    def remove_redundant_locus(
        full_set: DataFrame, study_locus_overlap: StudyLocusOverlap
    ) -> DataFrame:
        """Remove redundant loci by testing they are truly independent.

        Args:
            full_set (DataFrame): Full set of positive and negative evidence of locus to gene associations. These include false negatives.
            study_locus_overlap (StudyLocusOverlap): Study locus overlap dataset to remove duplicated loci

        Returns:
            DataFrame: Full set of positive and negative evidence of locus to gene associations. False negatives are removed. # TODO rename
        """
        return (
            full_set.alias("left")
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

    @classmethod
    def as_l2g_gold_standard(
        cls: type[OpenTargetsL2GGoldStandard],
        gold_standard_curation: DataFrame,
        v2g: V2G,
        study_locus_overlap: StudyLocusOverlap,
        interactions: DataFrame,
    ) -> L2GGoldStandard:
        """Initialise L2GGoldStandard from source dataset.

        Args:
            gold_standard_curation (DataFrame): Gold standard curation dataframe, extracted from https://github.com/opentargets/genetics-gold-standards
            v2g (V2G): Variant to gene dataset to bring distance between a variant and a gene's TSS
            study_locus_overlap (StudyLocusOverlap): Study locus overlap dataset to remove duplicated loci
            interactions (DataFrame): Gene-gene interactions dataset to remove negative cases where the gene interacts with a positive gene

        Returns:
            L2GGoldStandard: L2G Gold Standard dataset
        """
        interactions_df = cls.process_gene_interactions(interactions)

        positive_set = cls.create_positive_set(gold_standard_curation)

        full_set = cls.create_full_set(positive_set, v2g)

        final_set = full_set.transform(
            # TODO: move logic to L2GGoldStandard
            cls.remove_redundant_locus,
            study_locus_overlap,
        ).transform(cls.remove_false_negatives, interactions_df)

        return L2GGoldStandard(
            _df=final_set,
            _schema=L2GGoldStandard.get_schema(),
        )
