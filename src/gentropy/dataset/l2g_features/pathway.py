"""Methods to generate features based on the pathways enriched for a study's diseases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.credible_set import CredibleSetLocus
from gentropy.dataset.l2g_features.distance import (
    common_neighbourhood_distance_feature_logic,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_features.vep import common_vep_feature_logic
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.pathway_enrichment import PathwayLibrary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def prioritised_disease_genes(
    study_locus: CredibleSetLocus,
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
    with no ontology expansion. Every gene is kept with the chromosome of its credible set, which
    the gene is on, so that the lists can leave a chromosome out.

    Args:
        study_locus (CredibleSetLocus): Credible sets; only the GWAS ones are used
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of distances and consequences
        target_index (TargetIndex): Target index, used to find the protein-coding genes
        vep_score_threshold (float): Smallest `vepMaximum` that prioritises a gene

    Returns:
        DataFrame: `diseaseId`, `chromosome` and `geneId`, one row per disease and prioritised
            gene
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
        .select("credibleSetId", "geneId")
    )
    coding_variant_genes = (
        common_vep_feature_logic(
            gwas_credible_sets,
            variant_index=variant_index,
            feature_name="vepMaximum",
        )
        .filter(f.col("vepMaximum") >= vep_score_threshold)
        .select("credibleSetId", "geneId")
    )
    return (
        nearest_genes.unionByName(coding_variant_genes)
        .join(
            gwas_credible_sets.df.select("credibleSetId", "studyId", "chromosome"),
            "credibleSetId",
            "inner",
        )
        .join(
            study_index.df.select(
                "studyId", f.explode("diseaseIds").alias("diseaseId")
            ),
            "studyId",
            "inner",
        )
        .select("diseaseId", "chromosome", "geneId")
        .distinct()
    )


def gwas_disease_sets(study_index: StudyIndex) -> DataFrame:
    """Sorted set of diseases of every GWAS study with at least one disease.

    Studies are grouped by their set of diseases rather than handled one by one: there are
    far fewer distinct disease sets than studies, and the great majority hold one disease.

    Args:
        study_index (StudyIndex): Study index

    Returns:
        DataFrame: `studyId` and `diseaseSet`
    """
    return (
        study_index.df.filter(f.col("studyType") == "gwas")
        .filter(f.size(f.col("diseaseIds")) > 0)
        .select("studyId", f.array_sort(f.col("diseaseIds")).alias("diseaseSet"))
        .distinct()
    )


@dataclass
class PathwayEnrichmentScores:
    """Pathway enrichment of every disease set, computed once and shared by the pathway features.

    The enrichment is release-wide and the same for every pathway feature, so the feature
    matrix step builds it once with `from_release`, hands it to the features through the
    feature input loader, and calls `unpersist` once the matrix is written.

    Attributes:
        scores (DataFrame): `diseaseSet`, `chromosome`, `geneId` and `score`, for genes in at
            least one enriched pathway, where `chromosome` is the chromosome of the gene and the
            one left out of the enrichment
        pathways_per_gene (DataFrame): `geneId` and `pathwayCount`, the number of library
            pathways the gene belongs to, for genes in at least one pathway
        gene_sets (DataFrame): `pathwayId` and `geneId`, the library the scores were built on
    """

    scores: DataFrame
    pathways_per_gene: DataFrame
    gene_sets: DataFrame

    @classmethod
    def from_release(
        cls: type[PathwayEnrichmentScores],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        *,
        pathway_library: PathwayLibrary,
        study_index: StudyIndex,
        study_locus: CredibleSetLocus,
        target_index: TargetIndex,
        variant_index: VariantIndex,
        p_value_adjusted_threshold: float = 0.05,
        min_pathway_size: int = 5,
        max_pathway_size: int = 4000,
        min_disease_genes: int = 25,
        vep_score_threshold: float = 0.66,
    ) -> PathwayEnrichmentScores:
        """Score every gene by how much of its pathway membership is enriched for each disease set.

        Pathways are tested for each disease from scratch:

        1. At every GWAS credible set the nearest gene and the genes hit by a coding variant
            are prioritised, see `prioritised_disease_genes`, and pooled per disease.
        2. For each chromosome, the genes on it are left out of the test, so that a locus is
            scored without its own genes or those of other credible sets nearby (leave one
            chromosome out).
        3. Every GO biological process and Reactome pathway of `pathway_library` with
            `min_pathway_size` to `max_pathway_size` protein-coding members, one per distinct
            gene set (see `PathwayLibrary.gene_sets`), is tested for enrichment among each
            disease's genes by a logistic score test adjusted for gene length, protein-coding
            TSS density and the gene's number of pathways, see `PathwayLibrary.score_test`.
            Diseases with fewer than
            `min_disease_genes` listed genes on all chromosomes together are not tested.
        4. A pathway counts as enriched for a disease below the adjusted p-value threshold, for
            the chromosome left out.

        For a gene the score is the fraction of the pathways it belongs to that are enriched
        for the diseases of the set. Pathways are counted once per disease set even when
        several of its diseases flag the same pathway, so the score falls between 0 and 1.

        The gene lists use every GWAS credible set in `study_locus`, not only the loci to
        annotate, so the score of a locus does not depend on which other loci are annotated
        with it. Scores are only built for the disease sets and chromosomes of the loci to
        annotate, since a locus only scores genes on its own chromosome.

        The three DataFrames are cached; call `unpersist` when done.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The study loci that will
                be annotated
            pathway_library (PathwayLibrary): GO and Reactome tables of the release
            study_index (StudyIndex): Study index, used to resolve a study to its diseases
            study_locus (CredibleSetLocus): Credible sets, the source of the disease gene lists
            target_index (TargetIndex): Target index, used for gene annotations
            variant_index (VariantIndex): Variant index, used to prioritise genes at each
                credible set
            p_value_adjusted_threshold (float): Largest adjusted p-value, exclusive, for a
                pathway to count as enriched
            min_pathway_size (int): Smallest number of member genes a tested pathway may have
            max_pathway_size (int): Largest number of member genes a tested pathway may have
            min_disease_genes (int): Smallest number of listed genes a disease needs to be
                tested
            vep_score_threshold (float): Smallest `vepMaximum` that prioritises a gene

        Returns:
            PathwayEnrichmentScores: Cached scores, pathway counts and gene sets
        """
        gene_sets = pathway_library.gene_sets(
            target_index, min_size=min_pathway_size, max_size=max_pathway_size
        ).cache()
        pathways_per_gene = (
            gene_sets.groupBy("geneId")
            .agg(f.count("pathwayId").alias("pathwayCount"))
            .cache()
        )
        # Only the disease sets and chromosomes of the loci to annotate are ever looked up.
        disease_set_chromosomes = (
            study_locus.df.select("credibleSetId", "studyId", "chromosome")
            .join(
                study_loci_to_annotate.df.select("credibleSetId").distinct(),
                "credibleSetId",
                "semi",
            )
            .join(gwas_disease_sets(study_index), "studyId", "inner")
            .select("diseaseSet", "chromosome")
            .distinct()
        )
        enriched_pathways = PathwayLibrary.score_test(
            prioritised_disease_genes(
                study_locus,
                study_index,
                variant_index,
                target_index,
                vep_score_threshold=vep_score_threshold,
            ),
            gene_sets,
            target_index,
            p_value_adjusted_threshold=p_value_adjusted_threshold,
            min_genes=min_disease_genes,
            held_out_chromosomes=sorted(
                row["chromosome"]
                for row in disease_set_chromosomes.select("chromosome")
                .distinct()
                .collect()
            ),
        ).select(
            "diseaseId", f.col("heldOutChromosome").alias("chromosome"), "pathwayId"
        )
        # A gene is only scored with the enrichment that left its own chromosome out.
        gene_chromosomes = target_index.df.select(
            f.col("id").alias("geneId"),
            f.col("genomicLocation.chromosome").alias("chromosome"),
        )
        enriched_pathways_per_gene = (
            disease_set_chromosomes.select(
                "diseaseSet", "chromosome", f.explode("diseaseSet").alias("diseaseId")
            )
            .join(enriched_pathways, ["diseaseId", "chromosome"], "inner")
            .select("diseaseSet", "chromosome", "pathwayId")
            .distinct()
            .join(
                gene_sets.join(gene_chromosomes, "geneId", "inner"),
                ["pathwayId", "chromosome"],
                "inner",
            )
            .groupBy("diseaseSet", "chromosome", "geneId")
            .agg(f.count("pathwayId").alias("enrichedPathwaysPerGene"))
        )
        scores = (
            enriched_pathways_per_gene.join(pathways_per_gene, "geneId", "inner")
            .select(
                "diseaseSet",
                "chromosome",
                "geneId",
                (f.col("enrichedPathwaysPerGene") / f.col("pathwayCount")).alias(
                    "score"
                ),
            )
            .cache()
        )
        return cls(
            scores=scores, pathways_per_gene=pathways_per_gene, gene_sets=gene_sets
        )

    def unpersist(self: PathwayEnrichmentScores) -> None:
        """Release the cached DataFrames."""
        for df in (self.scores, self.pathways_per_gene, self.gene_sets):
            df.unpersist()


def genes_in_window(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    study_locus: CredibleSetLocus,
    target_index: TargetIndex,
    genomic_window: int,
) -> DataFrame:
    """Every gene whose TSS lies within `genomic_window` of the lead variant of a locus to annotate.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The study loci that will be
            annotated
        study_locus (CredibleSetLocus): Credible sets, the source of each locus's position
        target_index (TargetIndex): Target index, the source of gene positions
        genomic_window (int): Distance up and downstream of the lead variant to collect genes
            from

    Returns:
        DataFrame: `credibleSetId`, `studyId`, `chromosome` and `geneId`
    """
    return (
        study_locus.df.select("credibleSetId", "studyId", "chromosome", "position")
        .join(
            study_loci_to_annotate.df.select("credibleSetId").distinct(),
            "credibleSetId",
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
        .select("credibleSetId", "studyId", "chromosome", "geneId")
    )


def common_pathway_enrichment_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pathway_scores: PathwayEnrichmentScores,
    study_index: StudyIndex,
    study_locus: CredibleSetLocus,
    target_index: TargetIndex,
    genomic_window: int,
) -> DataFrame:
    """Score every gene at a locus by how much of its pathway membership is disease relevant.

    The score of a gene is the fraction of the pathways it belongs to that are enriched for
    the diseases of the study behind the credible set, see
    `PathwayEnrichmentScores.from_release`. A gene that sits in ten pathways of which three are
    enriched scores 0.3; a gene in the window that no enriched pathway contains, or that is in
    no pathway at all, scores 0. `pathwayCount` tells the two apart.

    The test adjusts for annotation depth, so this score carries only the disease-specific part;
    `pathwayCount` carries the generic one. The two are meant to be used together, though either
    can be requested alone.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The study loci that will be
            annotated
        feature_name (str): The name of the feature
        pathway_scores (PathwayEnrichmentScores): Enrichment scores of every disease set, built
            for these loci to annotate
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        study_locus (CredibleSetLocus): Credible sets, the source of the position of the study
            locus
        target_index (TargetIndex): Target index, used for gene positions
        genomic_window (int): Distance up and downstream of the study locus to collect genes from

    Returns:
        DataFrame: Feature dataset with one row per study locus and gene in its window
    """
    return (
        genes_in_window(study_loci_to_annotate, study_locus, target_index, genomic_window)
        .join(gwas_disease_sets(study_index), "studyId", "left")
        .join(pathway_scores.scores, ["diseaseSet", "chromosome", "geneId"], "left")
        .select(
            "credibleSetId",
            "geneId",
            f.coalesce(f.col("score"), f.lit(0.0)).alias(feature_name),
        )
        .distinct()
    )


def common_neighbourhood_pathway_enrichment_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    **kwargs: Any,
) -> DataFrame:
    """Rank the pathway enrichment score of a gene against the other genes at the same locus.

    The score itself is dense - most genes in a window belong to at least one enriched
    pathway - so what distinguishes genes is how they compare with their neighbours. This
    divides each gene's score by the largest score at the locus.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci
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
    regional_max = f.max(local_feature_name).over(Window.partitionBy("credibleSetId"))
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


def common_pathway_count_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pathway_scores: PathwayEnrichmentScores,
    study_locus: CredibleSetLocus,
    target_index: TargetIndex,
    genomic_window: int,
) -> DataFrame:
    """Number of library pathways each gene at a locus belongs to.

    Counted on the library the enrichment scores were built on: within the pathway size limits
    and with identical gene sets collapsed into one. A gene in no pathway, including every
    gene that is not protein coding, counts 0. Next to the enrichment score this separates a
    gene that was tested and found in no enriched pathway from one with nothing to test.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The study loci that will be
            annotated
        feature_name (str): The name of the feature
        pathway_scores (PathwayEnrichmentScores): Holds the pathway count of every gene
        study_locus (CredibleSetLocus): Credible sets, the source of the position of the study
            locus
        target_index (TargetIndex): Target index, used for gene positions
        genomic_window (int): Distance up and downstream of the study locus to collect genes from

    Returns:
        DataFrame: Feature dataset with one row per study locus and gene in its window
    """
    return (
        genes_in_window(study_loci_to_annotate, study_locus, target_index, genomic_window)
        .join(pathway_scores.pathways_per_gene, "geneId", "left")
        .select(
            "credibleSetId",
            "geneId",
            f.coalesce(f.col("pathwayCount"), f.lit(0)).cast("float").alias(feature_name),
        )
        .distinct()
    )


class PathwayEnrichmentFeature(L2GFeature):
    """Fraction of a gene's pathways that are enriched for the diseases of the study."""

    feature_dependency_type = [
        PathwayEnrichmentScores,
        StudyIndex,
        CredibleSetLocus,
        TargetIndex,
    ]
    feature_name = "pathwayEnrichment500kb"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PathwayEnrichmentFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PathwayEnrichmentFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Pathway scores, studies, credible sets and genes

        Returns:
            PathwayEnrichmentFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_pathway_enrichment_feature_logic(
                    study_loci_to_annotate,
                    cls.feature_name,
                    genomic_window=cls.genomic_window,
                    **feature_dependency,
                ),
                id_vars=("credibleSetId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PathwayEnrichmentNeighbourhoodFeature(L2GFeature):
    """Pathway enrichment score of a gene relative to the maximum at the same locus."""

    feature_dependency_type = [
        PathwayEnrichmentScores,
        StudyIndex,
        CredibleSetLocus,
        TargetIndex,
    ]
    feature_name = "pathwayEnrichment500kbNeighbourhood"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PathwayEnrichmentNeighbourhoodFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PathwayEnrichmentNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Pathway scores, studies, credible sets and genes

        Returns:
            PathwayEnrichmentNeighbourhoodFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_pathway_enrichment_feature_logic(
                    study_loci_to_annotate,
                    cls.feature_name,
                    genomic_window=cls.genomic_window,
                    **feature_dependency,
                ),
                id_vars=("credibleSetId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PathwayCountFeature(L2GFeature):
    """Number of library pathways a gene belongs to."""

    feature_dependency_type = [
        PathwayEnrichmentScores,
        CredibleSetLocus,
        TargetIndex,
    ]
    feature_name = "pathwayCount"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PathwayCountFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PathwayCountFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Pathway scores, credible sets and genes

        Returns:
            PathwayCountFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_pathway_count_feature_logic(
                    study_loci_to_annotate,
                    cls.feature_name,
                    genomic_window=cls.genomic_window,
                    **feature_dependency,
                ),
                id_vars=("credibleSetId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
