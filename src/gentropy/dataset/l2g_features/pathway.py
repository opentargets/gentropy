"""Methods to generate features based on the pathways enriched for a study's diseases."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.pathway_enrichment import PathwayEnrichment
from gentropy.dataset.pathway_index import PathwayIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def common_pathway_enrichment_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pathway_index: PathwayIndex,
    pathway_enrichment: PathwayEnrichment,
    study_index: StudyIndex,
    study_locus: StudyLocus,
    target_index: TargetIndex,
    p_value_adjusted_threshold: float,
    genomic_window: int,
) -> DataFrame:
    """Score every gene at a locus by how much of its pathway membership is disease relevant.

    For a gene the score is the fraction of the pathways it belongs to that are enriched
    among the genes associated with the diseases of the study behind the credible set. A gene
    that sits in ten pathways of which three are enriched scores 0.3; a gene in the window
    that no enriched pathway contains scores 0.

    Pathways are counted once per study even when several of the study's diseases flag the
    same pathway, and the denominator only counts pathways that enrichment was tested for, so
    the score always falls between 0 and 1.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the feature
        pathway_index (PathwayIndex): Gene set membership of the pathway library
        pathway_enrichment (PathwayEnrichment): Pathways enriched for each disease
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        study_locus (StudyLocus): Credible sets, used for the position of the study locus
        target_index (TargetIndex): Target index, used for gene positions
        p_value_adjusted_threshold (float): Maximum adjusted p-value for a pathway to count as
            enriched
        genomic_window (int): Distance up and downstream of the study locus to collect genes from

    Returns:
        DataFrame: Feature dataset with one row per study locus and gene in its window
    """
    # Gene set membership, restricted to the pathways enrichment was actually tested for. The
    # gene identifiers come from the index itself, which resolved them against a release of the
    # target index when it was built.
    membership = pathway_index.gene_membership().join(
        pathway_enrichment.tested_pathways(), "pathwayFromSourceName", "semi"
    )
    pathways_per_gene = membership.groupBy("geneId").agg(
        f.count("pathwayFromSourceName").alias("pathwaysPerGene")
    )

    # Studies are grouped by their set of diseases rather than handled one by one: there are
    # far fewer distinct disease sets than studies, and the great majority hold one disease.
    disease_sets = (
        study_index.df.filter(f.col("studyType") == "gwas")
        .filter(f.size(f.col("diseaseIds")) > 0)
        .select("studyId", f.array_sort(f.col("diseaseIds")).alias("diseaseSet"))
        .distinct()
    )
    enriched_pathways_per_gene = (
        disease_sets.select("diseaseSet")
        .distinct()
        .select("diseaseSet", f.explode("diseaseSet").alias("diseaseId"))
        .distinct()
        .join(
            pathway_enrichment.enriched_pathways(p_value_adjusted_threshold),
            "diseaseId",
            "inner",
        )
        .select("diseaseSet", "pathwayFromSourceName")
        .distinct()
        .join(membership, "pathwayFromSourceName", "inner")
        .groupBy("diseaseSet", "geneId")
        .agg(f.count("pathwayFromSourceName").alias("enrichedPathwaysPerGene"))
    )
    scores = enriched_pathways_per_gene.join(
        pathways_per_gene, "geneId", "inner"
    ).select(
        "diseaseSet",
        "geneId",
        (f.col("enrichedPathwaysPerGene") / f.col("pathwaysPerGene")).alias("score"),
    )

    genes_in_window = (
        study_locus.df.select("studyLocusId", "studyId", "chromosome", "position")
        .join(
            study_loci_to_annotate.df.select("studyLocusId").distinct(),
            "studyLocusId",
            "semi",
        )
        .join(
            target_index.df.select(
                f.col("id").alias("geneId"),
                f.col("genomicLocation.chromosome").alias("geneChromosome"),
                "tss",
            ),
            on=(f.col("chromosome") == f.col("geneChromosome"))
            & (f.abs(f.col("tss") - f.col("position")) <= genomic_window),
            how="inner",
        )
        .select("studyLocusId", "studyId", "geneId")
    )

    return (
        genes_in_window.join(disease_sets, "studyId", "left")
        .join(scores, ["diseaseSet", "geneId"], "left")
        .select(
            "studyLocusId",
            "geneId",
            f.coalesce(f.col("score"), f.lit(0.0)).alias(feature_name),
        )
        .distinct()
    )


def common_neighbourhood_pathway_enrichment_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    **kwargs: Any,
) -> DataFrame:
    """Rank the pathway enrichment score of a gene against the other genes at the same locus.

    The score itself is dense - most genes in a window belong to at least one enriched
    pathway - so what distinguishes genes is how they compare with their neighbours. This
    divides each gene's score by the largest score at the locus.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the neighbourhood feature, ending in "Neighbourhood"
        **kwargs (Any): Arguments of `common_pathway_enrichment_feature_logic`

    Returns:
        DataFrame: Feature dataset with one row per study locus and gene in its window
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_scores = common_pathway_enrichment_feature_logic(
        study_loci_to_annotate, local_feature_name, **kwargs
    )
    regional_max = f.max(local_feature_name).over(Window.partitionBy("studyLocusId"))
    return (
        local_scores.withColumn("regionalMax", regional_max)
        .withColumn(
            feature_name,
            f.when(
                f.col("regionalMax") > 0.0,
                f.col(local_feature_name) / f.col("regionalMax"),
            ).otherwise(f.lit(0.0)),
        )
        .drop("regionalMax", local_feature_name)
    )


class PathwayEnrichmentFeature(L2GFeature):
    """Fraction of a gene's pathways that are enriched for the diseases of the study."""

    feature_dependency_type = [
        PathwayIndex,
        PathwayEnrichment,
        StudyIndex,
        StudyLocus,
        TargetIndex,
    ]
    feature_name = "pathwayEnrichment500kb"
    p_value_adjusted_threshold: float = 0.05
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PathwayEnrichmentFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PathwayEnrichmentFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Datasets with the pathway library, the enrichment results, the studies, the credible sets and the genes

        Returns:
            PathwayEnrichmentFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_pathway_enrichment_feature_logic(
                    study_loci_to_annotate,
                    cls.feature_name,
                    p_value_adjusted_threshold=cls.p_value_adjusted_threshold,
                    genomic_window=cls.genomic_window,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PathwayEnrichmentNeighbourhoodFeature(L2GFeature):
    """Pathway enrichment score of a gene relative to the maximum at the same locus."""

    feature_dependency_type = [
        PathwayIndex,
        PathwayEnrichment,
        StudyIndex,
        StudyLocus,
        TargetIndex,
    ]
    feature_name = "pathwayEnrichment500kbNeighbourhood"
    p_value_adjusted_threshold: float = 0.05
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PathwayEnrichmentNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PathwayEnrichmentNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Datasets with the pathway library, the enrichment results, the studies, the credible sets and the genes

        Returns:
            PathwayEnrichmentNeighbourhoodFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_pathway_enrichment_feature_logic(
                    study_loci_to_annotate,
                    cls.feature_name,
                    p_value_adjusted_threshold=cls.p_value_adjusted_threshold,
                    genomic_window=cls.genomic_window,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
