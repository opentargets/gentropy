"""Fine-mapping-based polygenic priority score dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.l2g_features.distance import (
    DistanceSentinelTssNeighbourhoodFeature,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import StudyLocus
    from gentropy.dataset.target_index import TargetIndex
    from gentropy.dataset.variant_index import VariantIndex


@dataclass
class FmPopsScore(Dataset):
    """Gene-level prior learnt from gene features and the nearest genes of GWAS credible sets.

    One row per protein-coding gene with PoPS features. `targetCount` is the number of distinct
    diseases for which the gene is the nearest gene to the lead variant of at least one GWAS
    credible set, and `fmPops` is the out-of-chromosome prediction of `log(1 + targetCount)`
    from the PoPS gene features, with covariates projected out, as computed by
    [`FmPops`][gentropy.method.fm_pops.FmPops]. The score is centred on the average gene: a
    positive value means the gene looks, by its features, like genes that are nearest to many
    disease loci.

    | geneId            | chromosome | targetCount | fmPops |
    | ----------------- | ---------- | ----------- | ------ |
    | `ENSG00000169174` | `1`        | 7           | 0.412  |
    | `ENSG00000186092` | `1`        | 0           | -0.087 |

    The score is the same for every credible set and every study: it is a prior on the gene,
    not on the gene-disease pair.
    """

    @classmethod
    def get_schema(cls: type[FmPopsScore]) -> StructType:
        """Provide the schema for the FmPopsScore dataset.

        Returns:
            StructType: The schema of the FmPopsScore dataset.
        """
        return parse_spark_schema("fm_pops_score.json")

    @staticmethod
    def disease_counts_per_nearest_gene(
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Count the distinct diseases each gene is the nearest gene for.

        The nearest gene of a credible set is the protein-coding gene whose TSS is closest to
        the lead variant, i.e. the gene with `distanceSentinelTssNeighbourhood` equal to 1, so
        ties keep every tied gene. Only GWAS credible sets count, and a disease counts once per
        gene however many credible sets or studies point at it.

        TODO: related traits are counted as separate diseases, which inflates the counts of
        genes at loci shared by many lipid or blood-cell traits. Grouping diseases through the
        EFO hierarchy or by therapeutic area would temper that.

        Args:
            study_locus (StudyLocus): Credible sets
            study_index (StudyIndex): Study index, used to resolve a study to its diseases
            variant_index (VariantIndex): Variant index, the source of the distances to genes
            target_index (TargetIndex): Target index, used to keep protein-coding genes

        Returns:
            DataFrame: `geneId` and `targetCount`, for the genes nearest to at least one
                credible set of a study with a disease
        """
        gwas_credible_sets = study_locus.filter(f.col("studyType") == "gwas")
        nearest = (
            DistanceSentinelTssNeighbourhoodFeature.compute(
                study_loci_to_annotate=gwas_credible_sets,
                feature_dependency={
                    "variant_index": variant_index,
                    "target_index": target_index,
                },
            )
            .df.filter(f.col("featureValue") == 1.0)
            .select("studyLocusId", "geneId")
        )
        diseases = study_index.df.select(
            "studyId", f.explode("diseaseIds").alias("diseaseId")
        )
        return (
            nearest.join(
                gwas_credible_sets.df.select("studyLocusId", "studyId"),
                "studyLocusId",
                "inner",
            )
            .join(diseases, "studyId", "inner")
            .groupBy("geneId")
            .agg(f.countDistinct("diseaseId").cast("integer").alias("targetCount"))
        )

    @staticmethod
    def gene_covariates(
        target_index: TargetIndex, genomic_window: int = 500_000
    ) -> DataFrame:
        """Gene-level covariates of the target, for the protein-coding genes of a release.

        A gene in a gene desert is the nearest gene to every locus around it, so the number of
        diseases it is nearest for depends on its surroundings as much as on its biology. Two
        covariates capture that and are projected out of the target: the log of the gene length
        and the log of one plus the number of other protein-coding genes with a TSS within
        `genomic_window` of the gene's TSS.

        Args:
            target_index (TargetIndex): Target index
            genomic_window (int): Distance from the TSS within which neighbouring genes count

        Returns:
            DataFrame: `geneId`, `chromosome`, `tss`, `logGeneLength` and
                `logNeighbouringGenes`, one row per protein-coding gene with a TSS
        """
        genes = target_index.df.filter(
            (f.col("biotype") == "protein_coding") & f.col("tss").isNotNull()
        ).select(
            f.col("id").alias("geneId"),
            f.col("genomicLocation.chromosome").alias("chromosome"),
            "tss",
            f.log(
                f.greatest(
                    f.col("genomicLocation.end") - f.col("genomicLocation.start") + 1,
                    f.lit(1),
                )
            ).alias("logGeneLength"),
        )
        neighbours = genes.select(
            f.col("geneId").alias("neighbourId"),
            f.col("chromosome").alias("neighbourChromosome"),
            f.col("tss").alias("neighbourTss"),
        )
        neighbour_counts = (
            genes.join(
                neighbours,
                (f.col("chromosome") == f.col("neighbourChromosome"))
                & (f.abs(f.col("tss") - f.col("neighbourTss")) <= genomic_window)
                & (f.col("geneId") != f.col("neighbourId")),
                "inner",
            )
            .groupBy("geneId")
            .agg(f.count("neighbourId").alias("neighbouringGenes"))
        )
        return genes.join(neighbour_counts, "geneId", "left").select(
            "geneId",
            "chromosome",
            "tss",
            f.coalesce(f.col("logGeneLength"), f.lit(0.0)).alias("logGeneLength"),
            f.log1p(f.coalesce(f.col("neighbouringGenes"), f.lit(0))).alias(
                "logNeighbouringGenes"
            ),
        )
