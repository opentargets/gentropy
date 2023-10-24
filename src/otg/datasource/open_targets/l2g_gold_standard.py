"""Parser for OTPlatform locus to gene gold standards curation."""
from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from otg.common.spark_helpers import get_record_with_maximum_value
from otg.dataset.l2g.gold_standard import L2GGoldStandard
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G


class OpenTargetsL2GGoldStandard:
    """Parser for OTGenetics locus to gene gold standards curation.

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
        overlaps_df = study_locus_overlap._df.select(
            "leftStudyLocusId", "rightStudyLocusId"
        )
        interactions_df = cls.process_gene_interactions(interactions)
        return L2GGoldStandard(
            _df=(
                gold_standard_curation.filter(
                    f.col("gold_standard_info.highest_confidence").isin(
                        ["High", "Medium"]
                    )
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
                    StudyLocus.assign_study_locus_id("studyId", "variantId"),
                )
                .groupBy("studyLocusId", "studyId", "variantId", "geneId")
                .agg(
                    f.collect_set("source").alias("sources"),
                )
                # Assign Positive or Negative Status based on confidence
                .join(
                    v2g.df.filter(f.col("distance").isNotNull()).select(
                        "variantId", "geneId", "distance"
                    ),
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
                .select("studyLocusId", "geneId", "goldStandardSet", "sources")
            ),
            _schema=L2GGoldStandard.get_schema(),
        )
