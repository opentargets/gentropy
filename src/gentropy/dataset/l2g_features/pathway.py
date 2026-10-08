"""Methods to generate features based on the pathways enriched for a study's diseases."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.l2g_features.distance import (
    common_neighbourhood_distance_feature_logic,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_features.vep import common_vep_feature_logic
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.pathway_enrichment import PathwayLibrary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def prioritised_disease_genes(
    study_locus: StudyLocus,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    target_index: TargetIndex,
    vep_score_threshold: float = 0.66,
) -> DataFrame:
    """Genes prioritised by a simple rule at the GWAS credible sets of each disease.

    At every GWAS credible set two kinds of gene are prioritised: the protein-coding gene whose
    TSS is closest to the lead variant (`distanceSentinelTssNeighbourhood` equal to 1, so ties
    keep every tied gene), and any gene with a consequence of at least `vep_score_threshold`
    from a variant of the credible set (`vepMaximum`; 0.66 is a missense variant or worse).
    Each credible set's genes go to every disease in its study's `diseaseIds`, used as mapped,
    with no ontology expansion.

    Args:
        study_locus (StudyLocus): Credible sets; only the GWAS ones are used
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of distances and consequences
        target_index (TargetIndex): Target index, used to find the protein-coding genes
        vep_score_threshold (float): Smallest `vepMaximum` that prioritises a gene

    Returns:
        DataFrame: `diseaseId` and `geneId`, one row per disease and prioritised gene
    """
    gwas_credible_sets = study_locus.filter(f.col("studyType") == "gwas")
    nearest_genes = (
        common_neighbourhood_distance_feature_logic(
            gwas_credible_sets,
            variant_index=variant_index,
            feature_name="distanceSentinelTssNeighbourhood",
            distance_type="distanceFromTss",
            target_index=target_index,
        )
        .filter(f.col("distanceSentinelTssNeighbourhood") == 1.0)
        .select("studyLocusId", "geneId")
    )
    coding_variant_genes = (
        common_vep_feature_logic(
            gwas_credible_sets,
            variant_index=variant_index,
            feature_name="vepMaximum",
        )
        .filter(f.col("vepMaximum") >= vep_score_threshold)
        .select("studyLocusId", "geneId")
    )
    return (
        nearest_genes.unionByName(coding_variant_genes)
        .join(
            gwas_credible_sets.df.select("studyLocusId", "studyId"),
            "studyLocusId",
            "inner",
        )
        .join(
            study_index.df.select(
                "studyId", f.explode("diseaseIds").alias("diseaseId")
            ),
            "studyId",
            "inner",
        )
        .select("diseaseId", "geneId")
        .distinct()
    )


def common_pathway_enrichment_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pathway_library: PathwayLibrary,
    study_index: StudyIndex,
    study_locus: StudyLocus,
    target_index: TargetIndex,
    variant_index: VariantIndex,
    p_value_adjusted_threshold: float,
    genomic_window: int,
    min_pathway_size: int = 5,
    max_pathway_size: int = 4000,
    min_disease_genes: int = 25,
    vep_score_threshold: float = 0.66,
) -> DataFrame:
    """Score every gene at a locus by how much of its pathway membership is disease relevant.

    Pathways are tested for each disease from scratch:

    1. At every GWAS credible set the nearest gene and the genes hit by a coding variant are
        prioritised, see `prioritised_disease_genes`, and pooled per disease.
    2. Every GO biological process and Reactome pathway of `pathway_library` with
        `min_pathway_size` to `max_pathway_size` protein-coding members is tested for
        over-representation among each disease's genes, against the genes of the library, see
        `PathwayLibrary.over_representation`. Diseases with fewer than `min_disease_genes`
        genes in the library are not tested.
    3. A pathway counts as enriched for a disease below the adjusted p-value threshold.

    For a gene the score is the fraction of the pathways it belongs to that are enriched for
    the diseases of the study behind the credible set. A gene that sits in ten pathways of
    which three are enriched scores 0.3; a gene in the window that no enriched pathway
    contains, or that is in no pathway at all, scores 0.

    Pathways are counted once per study even when several of the study's diseases flag the
    same pathway, so the score always falls between 0 and 1.

    The gene lists use every GWAS credible set in `study_locus`, not only the loci to annotate,
    so the score of a locus does not depend on which other loci are being annotated with it.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the feature
        pathway_library (PathwayLibrary): GO and Reactome tables of the release
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        study_locus (StudyLocus): Credible sets, the source of the disease gene lists and of
            the position of the study locus
        target_index (TargetIndex): Target index, used for gene annotations and positions
        variant_index (VariantIndex): Variant index, used to prioritise genes at each credible
            set
        p_value_adjusted_threshold (float): Largest adjusted p-value, exclusive, for a pathway
            to count as enriched
        genomic_window (int): Distance up and downstream of the study locus to collect genes from
        min_pathway_size (int): Smallest number of member genes a tested pathway may have
        max_pathway_size (int): Largest number of member genes a tested pathway may have
        min_disease_genes (int): Smallest number of library genes a disease needs to be tested
        vep_score_threshold (float): Smallest `vepMaximum` that prioritises a gene

    Returns:
        DataFrame: Feature dataset with one row per study locus and gene in its window
    """
    gene_sets = pathway_library.gene_sets(
        target_index, min_size=min_pathway_size, max_size=max_pathway_size
    )
    pathways_per_gene = gene_sets.groupBy("geneId").agg(
        f.count("pathwayId").alias("pathwaysPerGene")
    )
    enriched_pathways = (
        PathwayLibrary.over_representation(
            prioritised_disease_genes(
                study_locus,
                study_index,
                variant_index,
                target_index,
                vep_score_threshold=vep_score_threshold,
            ),
            gene_sets,
            min_genes=min_disease_genes,
        )
        .filter(f.col("pValueAdjusted") < p_value_adjusted_threshold)
        .select("diseaseId", "pathwayId")
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
        .join(enriched_pathways, "diseaseId", "inner")
        .select("diseaseSet", "pathwayId")
        .distinct()
        .join(gene_sets, "pathwayId", "inner")
        .groupBy("diseaseSet", "geneId")
        .agg(f.count("pathwayId").alias("enrichedPathwaysPerGene"))
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
        PathwayLibrary,
        StudyIndex,
        StudyLocus,
        TargetIndex,
        VariantIndex,
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
            feature_dependency (dict[str, Any]): Pathway library, studies, credible sets, genes and variants

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
        PathwayLibrary,
        StudyIndex,
        StudyLocus,
        TargetIndex,
        VariantIndex,
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
            feature_dependency (dict[str, Any]): Pathway library, studies, credible sets, genes and variants

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
