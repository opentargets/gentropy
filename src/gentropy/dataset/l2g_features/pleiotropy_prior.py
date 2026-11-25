"""Methods to generate the predicted pleiotropy prior features from Open Targets gene data."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from functools import reduce
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.sparse
from pyspark.sql import DataFrame, SparkSession, Window

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.credible_set import CredibleSetLocus
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_features.other import is_protein_coding_feature_logic
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.pleiotropy_prior import PleiotropyPrior

logger = logging.getLogger(__name__)

# Distance from a gene's TSS within which other protein-coding TSSs count as its neighbours.
NEIGHBOURING_TSS_WINDOW = 500_000

# Largest distance between a credible-set variant and a gene footprint for the gene to get a
# feature value, the window of the `isProteinCoding` feature.
GENE_FOOTPRINT_WINDOW = 500_000

# Chromosomes whose genes are scored but kept out of the fits: many GWAS pipelines leave them
# out, so their genes are rarely anyone's nearest gene for reasons of coverage, not biology.
UNFITTED_CHROMOSOMES = ("X", "Y", "MT")


class GeneFeatureSource(Enum):
    """Sources of the gene features of the predicted pleiotropy prior."""

    GO = "go"
    REACTOME = "reactome"
    PPI = "ppi"
    MOUSE_PHENOTYPE = "mouse_phenotype"
    GTEX = "gtex"
    TABULA_SAPIENS = "tabula_sapiens"
    DICE = "dice"
    PRIDE = "pride"
    CONSTRAINT = "constraint"
    ESSENTIALITY = "essentiality"


# Baseline expression sources, each giving a level and a specificity block.
EXPRESSION_SOURCES = (
    GeneFeatureSource.GTEX,
    GeneFeatureSource.TABULA_SAPIENS,
    GeneFeatureSource.DICE,
    GeneFeatureSource.PRIDE,
)


@contextmanager
def arrow_transfers(spark: SparkSession) -> Iterator[None]:
    """Move data between Spark and pandas through Arrow inside the block.

    Without Arrow, every row collected by `toPandas` or sent by `createDataFrame` becomes a
    Python object, which for the gene features (tens of millions of rows) exhausts the driver.

    Args:
        spark (SparkSession): Spark session

    Yields:
        None: Nothing; the previous setting is restored on exit
    """
    key = "spark.sql.execution.arrow.pyspark.enabled"
    previous = spark.conf.get(key, None)
    spark.conf.set(key, "true")
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)


def nearest_gene_diseases(
    study_locus: CredibleSetLocus,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    target_index: TargetIndex,
) -> DataFrame:
    """Diseases each gene is the nearest gene for.

    The nearest gene of a credible set is the protein-coding gene whose TSS is closest to the
    lead variant, by the variant index's `distanceFromTss`; ties keep every tied gene. Only GWAS
    credible sets count. Diseases are the ontology terms in the
    `diseaseIds` field of the study index, used as mapped, with no expansion to ancestors.

    Args:
        study_locus (CredibleSetLocus): Credible sets
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        target_index (TargetIndex): Target index, used to keep protein-coding genes

    Returns:
        DataFrame: Distinct `geneId` and `diseaseId` pairs
    """
    protein_coding = target_index.df.filter(
        f.col("biotype") == "protein_coding"
    ).select(f.col("id").alias("geneId"))
    distances = (
        study_locus.df.filter(f.col("studyType") == StudyType.GWAS.value)
        .select("credibleSetId", "studyId", "variantId")
        .join(
            variant_index.get_distance_to_gene(distance_type="distanceFromTss").select(
                "variantId",
                f.col("targetId").alias("geneId"),
                f.abs("distanceFromTss").alias("distance"),
            ),
            "variantId",
            "inner",
        )
        .join(protein_coding, "geneId", "semi")
        .groupBy("credibleSetId", "studyId", "geneId")
        .agg(f.min("distance").alias("distance"))
    )
    nearest = distances.withColumn(
        "nearestDistance", f.min("distance").over(Window.partitionBy("credibleSetId"))
    ).filter(f.col("distance") == f.col("nearestDistance"))
    diseases = study_index.df.select(
        "studyId", f.explode("diseaseIds").alias("diseaseId")
    )
    return (
        nearest.join(diseases, "studyId", "inner")
        .select("geneId", "diseaseId")
        .distinct()
    )


def nearest_gene_disease_counts(
    study_locus: CredibleSetLocus,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    target_index: TargetIndex,
) -> DataFrame:
    """Count the distinct diseases each gene is the nearest gene for.

    The pairs come from
    [`nearest_gene_diseases`][gentropy.dataset.l2g_features.pleiotropy_prior.nearest_gene_diseases].
    A study mapped to several terms contributes each of them, and a term counts once per gene
    however many credible sets or studies point at it.

    TODO: related traits mapped to different terms are counted separately, which inflates the
    counts of genes at loci shared by many lipid or blood-cell traits. Grouping terms through
    the EFO hierarchy or by therapeutic area would temper that.

    Args:
        study_locus (CredibleSetLocus): Credible sets
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        target_index (TargetIndex): Target index, used to keep protein-coding genes

    Returns:
        DataFrame: `geneId` and `targetCount`, for the genes nearest to at least one credible
            set of a study with a disease
    """
    return (
        nearest_gene_diseases(study_locus, study_index, variant_index, target_index)
        .groupBy("geneId")
        .agg(f.count("*").cast("integer").alias("targetCount"))
    )


def gene_covariates(
    target_index: TargetIndex, genomic_window: int = NEIGHBOURING_TSS_WINDOW
) -> DataFrame:
    """Background genes of the prior and the positional covariates of its target.

    The background is every protein-coding gene with a chromosome and a TSS. A gene in a gene
    desert is the nearest gene to every locus around it, so the number of diseases it is
    nearest for depends on its surroundings as much as on its biology. Two covariates capture
    that and are projected out of the target: `log(1 + |end - start|)` and the log of one plus
    the number of protein-coding TSSs, the gene's own included, within `genomic_window` of the
    gene's TSS.

    Args:
        target_index (TargetIndex): Target index
        genomic_window (int): Distance from the TSS within which TSSs count

    Returns:
        DataFrame: `geneId`, `chromosome`, `tss`, `logGeneLength` and `logNeighbouringGenes`,
            one row per background gene
    """
    protein_coding = target_index.df.filter(
        f.col("biotype") == "protein_coding"
    ).select(f.col("id").alias("geneId"))
    genes = (
        target_index.locations_lut()
        .join(protein_coding, "geneId", "semi")
        .filter(f.col("tss").isNotNull() & f.col("chromosome").isNotNull())
        .select(
            "geneId",
            "chromosome",
            "tss",
            f.log1p(f.coalesce(f.abs(f.col("end") - f.col("start")), f.lit(0))).alias(
                "logGeneLength"
            ),
        )
    )
    neighbours = genes.select(
        f.col("chromosome").alias("neighbourChromosome"),
        f.col("tss").alias("neighbourTss"),
    )
    neighbour_counts = (
        genes.join(
            neighbours,
            (f.col("chromosome") == f.col("neighbourChromosome"))
            & (f.abs(f.col("tss") - f.col("neighbourTss")) <= genomic_window),
            "inner",
        )
        .groupBy("geneId")
        .agg(f.count("*").alias("neighbouringGenes"))
    )
    return genes.join(neighbour_counts, "geneId", "inner").select(
        "geneId",
        "chromosome",
        "tss",
        "logGeneLength",
        f.log1p("neighbouringGenes").alias("logNeighbouringGenes"),
    )


@dataclass
class GeneData:
    """Background genes of the priors and their features, held on the driver.

    Every row-indexed attribute follows the order of `genes`, and every column-indexed one the
    order of `feature_ids`.

    Attributes:
        genes (pd.DataFrame): Output of `gene_covariates`, sorted by `geneId`
        matrix (scipy.sparse.csc_array): `genes x features` matrix, not standardised
        feature_ids (list[str]): Identifier of every column of `matrix`
        feature_blocks (list[str]): Block of every column of `matrix`: its source, with the
            expression level and specificity of a source apart
        feature_sources (list[str]): Source of every column of `matrix`
        covariates (np.ndarray): `genes x covariates` positional covariates and source
            coverage flags
        covariate_names (list[str]): Name of every column of `covariates`
        out_of_fit (np.ndarray): Whether the gene is scored but kept out of the fits: its TSS
            lies in the MHC or it sits on one of `UNFITTED_CHROMOSOMES`
    """

    genes: pd.DataFrame
    matrix: scipy.sparse.csc_array
    feature_ids: list[str]
    feature_blocks: list[str]
    feature_sources: list[str]
    covariates: np.ndarray
    covariate_names: list[str]
    out_of_fit: np.ndarray

    def __post_init__(self: GeneData) -> None:
        """Check that the attributes describe the same genes and features.

        Raises:
            ValueError: If the attributes disagree on the number of genes or features
        """
        n_genes = len(self.genes)
        if (
            self.matrix.shape[0] != n_genes
            or self.covariates.shape[0] != n_genes
            or self.out_of_fit.shape[0] != n_genes
        ):
            raise ValueError("Gene data attributes disagree on the number of genes.")
        if not (
            self.matrix.shape[1]
            == len(self.feature_ids)
            == len(self.feature_blocks)
            == len(self.feature_sources)
        ):
            raise ValueError("Gene data attributes disagree on the number of features.")


class PleiotropyPriorInputs:
    """Open Targets gene data the predicted pleiotropy prior and approxPops are fitted on.

    The prior is one number per protein-coding gene saying how much the gene looks like the
    genes GWAS credible sets point to. It follows the polygenic priority score (PoPS; Weeks et
    al. 2023, Nat Genet 55:1267-1276), with the MAGMA target replaced by
    `y_g = log(1 + c_g)`, where `c_g` is the number of distinct diseases gene `g` is the
    nearest gene for (see
    [`nearest_gene_disease_counts`][gentropy.dataset.l2g_features.pleiotropy_prior.nearest_gene_disease_counts]),
    and the PoPS gene features replaced by features built from the release:

    | Block            | Source                                     | Features                                         |
    | ---------------- | ------------------------------------------ | ------------------------------------------------ |
    | GO               | `target.go[].id`, all evidence codes      | membership of each term                          |
    | Reactome         | `target.pathways[].pathwayId`              | membership of each pathway                       |
    | PPI              | `interaction`, STRING >= 0.7, IntAct >= 0.5 by default | first-degree neighbours of each hub gene |
    | Mouse phenotypes | `mouse_phenotype`                          | membership of each phenotype term                |
    | Expression       | `baseline_expression`                      | level and within-source specificity per context |
    | Constraint       | `target.constraint`                        | LoF oeUpper (LOEUF); missense oe, score; synonymous score |
    | Essentiality     | `target_essentiality.isEssential`          | 0/1                                              |

    Binary blocks keep terms with `min_term_size` to `max_term_size` (10 to 2,000 by default)
    member genes, counted among protein-coding genes. After standardisation a member of an
    `m`-gene term out of `n` genes gets `sqrt((n - m) / m)`, about 45 at `m = 10` and 3.6 at
    `m = 1,400`, so the smallest terms kept carry the largest values and the lower cutoff
    matters more than the upper one. The observed/expected ratios (LOEUF and missense oe) are
    capped at 2: above it sit tiny genes with a handful of expected variants, whose ratios
    gnomAD flags as unreliable and which would otherwise lead the top principal components.
    The LoF point estimate and pLI-like score are left out, LOEUF being the recommended LoF
    metric. Missing constraint values take the mean of the genes that have one, which is 0 after
    standardisation, so they add nothing to the kernel; the constraint coverage flag marks the
    genes with a LOEUF. Every column is standardised across all genes, those kept out of the fit
    included, and columns without variance are dropped; there is no block weighting and no
    feature selection.

    The target is regressed on the features with the leave-one-chromosome-out kernel ridge
    regression of [`PleiotropyPrior`][gentropy.method.pleiotropy_prior.PleiotropyPrior], with an
    intercept, the two positional covariates of
    [`gene_covariates`][gentropy.dataset.l2g_features.pleiotropy_prior.gene_covariates] and one
    0/1 coverage flag per source projected out within each fold. Genes in the major
    histocompatibility complex, and on chromosomes X, Y and MT, are scored but kept out of
    every training fold: credible sets with a lead variant in the MHC are flagged and usually
    excluded from a release, and many GWAS pipelines leave out the sex and mitochondrial
    chromosomes, so these genes are rarely anyone's nearest gene for reasons that have nothing
    to do with their biology. No L2G score and no gold-standard label enters the prior.

    The same gene features also give approxPops, a per-disease version closer to PoPS. For
    every disease with at least `approx_pops_min_genes` nearest genes, the 0/1 label "is a
    nearest gene of this disease", with the same covariates projected out, is regressed on the
    top `approx_pops_components` principal components of the features, with the leave-one-out
    scores, AUC filter and z-scoring of
    [`PleiotropyPrior.approx_pops`][gentropy.method.pleiotropy_prior.PleiotropyPrior.approx_pops].
    Before the components are taken, every feature is weighted by `1 / sqrt(block size)`, so
    each block (GO, Reactome, PPI, mouse phenotypes, expression level and specificity per
    source, constraint, essentiality) carries the same total variance; otherwise the 12,000 PPI
    columns would dominate. With `approx_pops_group_expression_by_source`, the level and
    specificity of a source form one block, so expression has four blocks rather than eight.
    As for the prior, the genes kept out of the fit are scored but do not enter it; for
    approxPops the features are standardised over the fitted genes only.

    The fits run on the Spark driver, from
    [`PleiotropyPriorScores.from_release`][gentropy.dataset.l2g_features.pleiotropy_prior.PleiotropyPriorScores.from_release]:
    the kernel over about 20,000 genes takes 1.6 GB in single precision, and each fold holds a
    copy of its training block and its eigenvectors on top.
    """

    CONSTRAINT_FEATURES = (
        ("lof", "oeUpper"),
        ("mis", "oe"),
        ("mis", "score"),
        ("syn", "score"),
    )
    CONSTRAINT_CAPS = {("lof", "oeUpper"): 2.0, ("mis", "oe"): 2.0}

    def __init__(
        self: PleiotropyPriorInputs,
        *,
        interactions: DataFrame,
        mouse_phenotype: DataFrame,
        baseline_expression: DataFrame,
        target_essentiality: DataFrame,
        lambda_grid: list[float] | None = None,
        min_term_size: int = 10,
        max_term_size: int = 2_000,
        string_min_score: float = 0.7,
        intact_min_score: float = 0.5,
        approx_pops_min_genes: int = 50,
        approx_pops_components: int = 100,
        approx_pops_group_expression_by_source: bool = False,
    ) -> None:
        """Hold the gene data of a release.

        Args:
            interactions (DataFrame): Open Targets `interaction` dataset
            mouse_phenotype (DataFrame): Open Targets `mouse_phenotype` dataset
            baseline_expression (DataFrame): Open Targets `baseline_expression` dataset
            target_essentiality (DataFrame): Open Targets `target_essentiality` dataset
            lambda_grid (list[float] | None): Ridge penalties searched by generalised
                cross-validation. Defaults to `10^-2` to `10^10` in steps of `10^0.5`.
            min_term_size (int): Fewest protein-coding member genes a GO term, Reactome
                pathway, mouse phenotype or interaction hub needs to become a feature
            max_term_size (int): Most protein-coding member genes it may have
            string_min_score (float): Lowest STRING score of a high-confidence interaction
            intact_min_score (float): Lowest IntAct score of a high-confidence interaction
            approx_pops_min_genes (int): Fewest nearest genes a disease needs to get an
                approxPops score
            approx_pops_components (int): Number of principal components of the gene features
                approxPops regresses on
            approx_pops_group_expression_by_source (bool): Whether the expression level and
                specificity of a source form one block of the approxPops weighting, rather than
                two
        """
        self.interactions = interactions
        self.mouse_phenotype = mouse_phenotype
        self.baseline_expression = baseline_expression
        self.target_essentiality = target_essentiality
        self.lambda_grid = lambda_grid
        self.min_term_size = min_term_size
        self.max_term_size = max_term_size
        self.string_min_score = string_min_score
        self.intact_min_score = intact_min_score
        self.approx_pops_min_genes = approx_pops_min_genes
        self.approx_pops_components = approx_pops_components
        self.approx_pops_group_expression_by_source = (
            approx_pops_group_expression_by_source
        )

    def gene_features(
        self: PleiotropyPriorInputs, genes: DataFrame, target_index: TargetIndex
    ) -> tuple[DataFrame, DataFrame]:
        """Build the gene features and the per-source coverage of the background genes.

        Args:
            genes (DataFrame): Background genes, with a `geneId` column
            target_index (TargetIndex): Target index, the source of GO, Reactome and constraint

        Coverage of GO, Reactome, PPI and mouse phenotypes means the gene has any annotation
        in the source, before the term-size filter, so a gene can be covered by a source that
        gives it no feature column.

        Returns:
            tuple[DataFrame, DataFrame]: Features as `geneId`, `featureId`, `source`, `block`,
                `value` rows, and coverage as distinct `geneId`, `source` pairs
        """
        background = genes.select("geneId")
        targets = target_index.df.withColumnRenamed("id", "geneId").join(
            background, "geneId", "semi"
        )

        memberships = (
            targets.select(
                "geneId",
                f.lit(GeneFeatureSource.GO.value).alias("source"),
                f.explode("go.id").alias("term"),
            )
            .unionByName(
                targets.select(
                    "geneId",
                    f.lit(GeneFeatureSource.REACTOME.value).alias("source"),
                    f.explode("pathways.pathwayId").alias("term"),
                )
            )
            .unionByName(self._ppi_neighbours(background))
            .unionByName(
                self.mouse_phenotype.select(
                    f.col("targetFromSourceId").alias("geneId"),
                    f.lit(GeneFeatureSource.MOUSE_PHENOTYPE.value).alias("source"),
                    f.col("modelPhenotypeId").alias("term"),
                ).join(background, "geneId", "semi")
            )
            .filter(f.col("term").isNotNull())
            .distinct()
        )
        term_size = Window.partitionBy("source", "term")
        binary = (
            memberships.withColumn("termSize", f.count("*").over(term_size))
            .filter(f.col("termSize").between(self.min_term_size, self.max_term_size))
            .select(
                "geneId",
                f.concat_ws(":", "source", "term").alias("featureId"),
                "source",
                f.col("source").alias("block"),
                f.lit(1.0).alias("value"),
            )
        )

        expression = self._expression(background)
        constraint = self._constraint(targets, background)
        essentiality_source = GeneFeatureSource.ESSENTIALITY.value
        essentiality = self.target_essentiality.select(
            f.col("targetId").alias("geneId"),
            f.lit(f"{essentiality_source}:isEssential").alias("featureId"),
            f.lit(essentiality_source).alias("source"),
            f.lit(essentiality_source).alias("block"),
            f.col("isEssential").cast("double").alias("value"),
        ).join(background, "geneId", "semi")

        columns = ["geneId", "featureId", "source", "block", "value"]
        features = (
            binary.unionByName(expression.select(*columns))
            .unionByName(constraint)
            .unionByName(essentiality)
        )
        coverage = (
            memberships.select("geneId", "source")
            .unionByName(expression.select("geneId", "source"))
            .unionByName(
                targets.select("geneId", f.explode("constraint").alias("constraint"))
                .filter(
                    (f.col("constraint.constraintType") == "lof")
                    & f.col("constraint.oeUpper").isNotNull()
                )
                .select(
                    "geneId", f.lit(GeneFeatureSource.CONSTRAINT.value).alias("source")
                )
            )
            .unionByName(
                essentiality.select(
                    "geneId", f.lit(essentiality_source).alias("source")
                )
            )
            .distinct()
        )
        return features, coverage

    def _ppi_neighbours(
        self: PleiotropyPriorInputs, background: DataFrame
    ) -> DataFrame:
        """First-degree neighbours of every hub gene in the high-confidence interaction network.

        Args:
            background (DataFrame): Background genes, with a `geneId` column

        Returns:
            DataFrame: `geneId` (the neighbour), `source` and `term` (the hub)
        """
        edges = self.interactions.filter(
            (
                (f.col("sourceDatabase") == "string")
                & (f.col("scoring") >= self.string_min_score)
            )
            | (
                (f.col("sourceDatabase") == "intact")
                & (f.col("scoring") >= self.intact_min_score)
            )
        ).filter(f.col("targetA") != f.col("targetB"))
        undirected = edges.select(
            f.col("targetA").alias("hub"), f.col("targetB").alias("geneId")
        ).unionByName(
            edges.select(
                f.col("targetB").alias("hub"), f.col("targetA").alias("geneId")
            )
        )
        return (
            undirected.join(background, "geneId", "semi")
            .join(background.withColumnRenamed("geneId", "hub"), "hub", "semi")
            .select(
                "geneId",
                f.lit(GeneFeatureSource.PPI.value).alias("source"),
                f.col("hub").alias("term"),
            )
        )

    def _expression(self: PleiotropyPriorInputs, background: DataFrame) -> DataFrame:
        """Expression level and within-source specificity of every gene in every context.

        A context is a tissue and cell type pair of one source. The level is
        `log(1 + max(median, 0))`, averaged over repeated measurements of the context; a
        measurement without a median is left out rather than read as 0. The specificity is the
        level z-scored across the contexts of the same source in which the gene is measured,
        and 0 when the gene's levels do not vary.

        Args:
            background (DataFrame): Background genes, with a `geneId` column

        Returns:
            DataFrame: `geneId`, `source`, `featureId`, `block` and `value`
        """
        levels = (
            self.baseline_expression.withColumn(
                "source", f.lower(f.col("datasourceId"))
            )
            .filter(f.col("source").isin(*[s.value for s in EXPRESSION_SOURCES]))
            .filter(f.col("median").isNotNull())
            .select(
                f.col("targetId").alias("geneId"),
                "source",
                f.concat_ws(
                    "|",
                    f.coalesce(f.col("tissueBiosampleId"), f.lit("")),
                    f.coalesce(f.col("celltypeBiosampleId"), f.lit("")),
                ).alias("context"),
                f.log1p(f.greatest(f.col("median"), f.lit(0.0))).alias("level"),
            )
            .join(background, "geneId", "semi")
            .groupBy("geneId", "source", "context")
            .agg(f.mean("level").alias("level"))
        )
        gene_in_source = Window.partitionBy("geneId", "source")
        sd = f.stddev_pop("level").over(gene_in_source)
        specificity = f.when(
            sd > 0, (f.col("level") - f.mean("level").over(gene_in_source)) / sd
        ).otherwise(f.lit(0.0))
        return levels.select(
            "geneId",
            "source",
            f.concat_ws(":", "source", f.lit("level"), "context").alias("featureId"),
            f.concat_ws(":", "source", f.lit("level")).alias("block"),
            f.col("level").alias("value"),
        ).unionByName(
            levels.select(
                "geneId",
                "source",
                f.concat_ws(":", "source", f.lit("specificity"), "context").alias(
                    "featureId"
                ),
                f.concat_ws(":", "source", f.lit("specificity")).alias("block"),
                specificity.alias("value"),
            )
        )

    def _constraint(
        self: PleiotropyPriorInputs, targets: DataFrame, background: DataFrame
    ) -> DataFrame:
        """Constraint metrics of every background gene, capped, missing values set to the mean.

        Args:
            targets (DataFrame): Target index rows of the background genes, `id` as `geneId`
            background (DataFrame): Background genes, with a `geneId` column

        Returns:
            DataFrame: `geneId`, `featureId`, `source`, `block` and `value`
        """
        source = GeneFeatureSource.CONSTRAINT.value
        constraints = targets.select(
            "geneId", f.explode("constraint").alias("constraint")
        ).select("geneId", "constraint.*")
        observed = reduce(
            DataFrame.unionByName,
            [
                constraints.filter(f.col("constraintType") == constraint_type).select(
                    "geneId",
                    f.lit(f"{source}:{constraint_type}:{field}").alias("featureId"),
                    # `least` skips nulls, so a missing value must stay null explicitly.
                    (
                        f.when(
                            f.col(field).isNotNull(),
                            f.least(
                                f.col(field).cast("double"),
                                f.lit(self.CONSTRAINT_CAPS[(constraint_type, field)]),
                            ),
                        )
                        if (constraint_type, field) in self.CONSTRAINT_CAPS
                        else f.col(field).cast("double")
                    ).alias("observed"),
                )
                for constraint_type, field in self.CONSTRAINT_FEATURES
            ],
        )
        feature_ids = background.sparkSession.createDataFrame(
            [
                (f"{source}:{constraint_type}:{field}",)
                for constraint_type, field in self.CONSTRAINT_FEATURES
            ],
            "featureId string",
        )
        return (
            background.crossJoin(feature_ids)
            .join(
                observed.groupBy("geneId", "featureId").agg(
                    f.mean("observed").alias("observed")
                ),
                ["geneId", "featureId"],
                "left",
            )
            .select(
                "geneId",
                "featureId",
                f.lit(source).alias("source"),
                f.lit(source).alias("block"),
                f.coalesce(
                    f.col("observed"),
                    f.mean("observed").over(Window.partitionBy("featureId")),
                ).alias("value"),
            )
        )

    def load_gene_data(
        self: PleiotropyPriorInputs, target_index: TargetIndex
    ) -> GeneData:
        """Collect the background genes, their covariates and their features on the driver.

        Args:
            target_index (TargetIndex): Target index

        Returns:
            GeneData: Genes, covariates and the sparse feature matrix, in the same gene order
        """
        spark = target_index.df.sparkSession
        with arrow_transfers(spark):
            genes = (
                gene_covariates(target_index)
                .toPandas()
                .sort_values("geneId")
                .reset_index(drop=True)
            )
            gene_index = spark.createDataFrame(
                genes[["geneId"]].assign(
                    geneIndex=np.arange(len(genes), dtype=np.int64)
                )
            )
            features, coverage = self.gene_features(
                spark.createDataFrame(genes[["geneId"]]), target_index
            )
            columns = (
                features.select("featureId", "source", "block")
                .distinct()
                .toPandas()
                .sort_values("featureId")
                .reset_index(drop=True)
            )
            feature_index = spark.createDataFrame(
                columns[["featureId"]].assign(
                    featureIndex=np.arange(len(columns), dtype=np.int64)
                )
            )
            entries = (
                features.join(gene_index, "geneId", "inner")
                .join(feature_index, "featureId", "inner")
                .select("geneIndex", "featureIndex", "value")
                .toPandas()
            )
            coverage_rows = coverage.toPandas()
        matrix = scipy.sparse.csc_array(
            (
                entries["value"].to_numpy(dtype=np.float64),
                (
                    entries["geneIndex"].to_numpy(dtype=np.int64),
                    entries["featureIndex"].to_numpy(dtype=np.int64),
                ),
            ),
            shape=(len(genes), len(columns)),
        )
        flags = (
            coverage_rows.assign(covered=1.0)
            .pivot_table(
                index="geneId", columns="source", values="covered", fill_value=0.0
            )
            .reindex(genes["geneId"], fill_value=0.0)
        )
        covariates = np.column_stack(
            [
                genes[["logGeneLength", "logNeighbouringGenes"]].to_numpy(),
                flags.to_numpy(dtype=np.float64),
            ]
        )
        mhc = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)
        out_of_fit = (
            (
                (genes["chromosome"] == mhc.chromosome)
                & genes["tss"].between(mhc.start, mhc.end)
            )
            | genes["chromosome"].isin(UNFITTED_CHROMOSOMES)
        ).to_numpy()
        return GeneData(
            genes=genes,
            matrix=matrix,
            feature_ids=columns["featureId"].tolist(),
            feature_blocks=columns["block"].tolist(),
            feature_sources=columns["source"].tolist(),
            covariates=covariates,
            covariate_names=["logGeneLength", "logNeighbouringGenes", *flags.columns],
            out_of_fit=out_of_fit,
        )

    def fit_prior(
        self: PleiotropyPriorInputs,
        data: GeneData,
        study_locus: CredibleSetLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Fit the predicted pleiotropy prior on the driver.

        Args:
            data (GeneData): Gene data, from `load_gene_data`
            study_locus (CredibleSetLocus): All credible sets of the release
            study_index (StudyIndex): Study index
            variant_index (VariantIndex): Variant index
            target_index (TargetIndex): Target index

        Returns:
            DataFrame: `geneId` and `predictedPleiotropyPrior`
        """
        spark = study_locus.df.sparkSession
        with arrow_transfers(spark):
            counts = (
                nearest_gene_disease_counts(
                    study_locus, study_index, variant_index, target_index
                )
                .toPandas()
                .set_index("geneId")["targetCount"]
            )
        target_count = (
            counts.reindex(data.genes["geneId"]).fillna(0).to_numpy(dtype=np.float64)
        )
        kernel, n_features = PleiotropyPrior.accumulate_kernel(data.matrix)
        # The fit runs in single precision; dropping the double-precision kernel halves memory.
        kernel = kernel.astype(np.float32)
        fit = PleiotropyPrior.loco_kernel_ridge(
            kernel,
            np.log1p(target_count),
            data.genes["chromosome"].to_numpy(dtype=str),
            covariates=data.covariates,
            fit_mask=~data.out_of_fit,
            lambdas=self.lambda_grid or PleiotropyPrior.DEFAULT_LAMBDA_GRID,
        )
        del kernel
        logger.info(
            "Predicted pleiotropy prior: %d genes, %d features (%d non-constant), "
            "covariates %s, %d genes nearest to at least one disease, %d genes in the MHC "
            "or on chromosomes %s kept out of the fit.",
            len(data.genes),
            len(data.feature_ids),
            n_features,
            ", ".join(data.covariate_names),
            int((target_count > 0).sum()),
            int(data.out_of_fit.sum()),
            "/".join(UNFITTED_CHROMOSOMES),
        )
        logger.info("Predicted pleiotropy prior: lambda %.3g", fit.penalty)
        for held_out, correlation in fit.held_out_correlation.items():
            logger.info(
                "Predicted pleiotropy prior: chromosome %s, held-out r %.3f",
                held_out,
                correlation,
            )

        with arrow_transfers(spark):
            return spark.createDataFrame(
                data.genes[["geneId"]].assign(predictedPleiotropyPrior=fit.scores),
                schema="geneId string, predictedPleiotropyPrior double",
            )

    def fit_approx_pops(
        self: PleiotropyPriorInputs,
        data: GeneData,
        study_locus: CredibleSetLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Fit approxPops on the driver.

        Args:
            data (GeneData): Gene data, from `load_gene_data`
            study_locus (CredibleSetLocus): All credible sets of the release
            study_index (StudyIndex): Study index
            variant_index (VariantIndex): Variant index
            target_index (TargetIndex): Target index

        Returns:
            DataFrame: `geneId`, `diseaseId` and `approxPops`; empty, with a warning, when no
                disease has enough nearest genes
        """
        spark = study_locus.df.sparkSession
        empty = spark.createDataFrame(
            [], "geneId string, diseaseId string, approxPops double"
        )
        fitted = ~data.out_of_fit
        # Fitted genes first, so that the kernel splits into contiguous blocks.
        order = np.concatenate([np.flatnonzero(fitted), np.flatnonzero(~fitted)])
        n_fit = int(fitted.sum())
        ordered_genes = data.genes["geneId"].to_numpy()[order]
        fit_genes = pd.Series(np.arange(n_fit), index=ordered_genes[:n_fit])

        with arrow_transfers(spark):
            pairs = nearest_gene_diseases(
                study_locus, study_index, variant_index, target_index
            ).toPandas()
        pairs = pairs[pairs["geneId"].isin(fit_genes.index)]
        sizes = pairs["diseaseId"].value_counts()
        diseases = np.sort(sizes.index[sizes >= self.approx_pops_min_genes].to_numpy())
        if diseases.size == 0:
            logger.warning(
                "approxPops: no disease has %d or more nearest genes; the approxPops "
                "features are left empty.",
                self.approx_pops_min_genes,
            )
            return empty
        pairs = pairs[pairs["diseaseId"].isin(diseases)]
        disease_position = pd.Series(np.arange(len(diseases)), index=diseases)
        labels = np.zeros((n_fit, len(diseases)))
        labels[
            fit_genes[pairs["geneId"]].to_numpy(),
            disease_position[pairs["diseaseId"]].to_numpy(),
        ] = 1.0

        matrix = data.matrix[order]
        reference_rows = np.arange(len(order)) < n_fit
        varying = PleiotropyPrior.varying_columns(matrix, reference_rows)
        blocks = pd.Series(
            data.feature_sources
            if self.approx_pops_group_expression_by_source
            else data.feature_blocks
        )
        block_size = blocks.map(blocks[varying].value_counts()).fillna(1)
        kernel, n_features = PleiotropyPrior.accumulate_kernel(
            matrix,
            column_weights=1 / np.sqrt(block_size.to_numpy(dtype=np.float64)),
            reference_rows=reference_rows,
        )
        kernel = kernel.astype(np.float32)
        eigenvalues, eigenvectors = PleiotropyPrior.top_components(
            kernel[:n_fit, :n_fit], self.approx_pops_components
        )
        fit = PleiotropyPrior.approx_pops(
            eigenvalues,
            eigenvectors,
            labels,
            covariates=data.covariates[order][:n_fit],
            lambdas=self.lambda_grid or PleiotropyPrior.DEFAULT_LAMBDA_GRID,
            held_out_kernel=kernel[n_fit:, :n_fit],
        )
        del kernel
        logger.info(
            "approxPops: %d genes fitted and %d more scored, %d non-constant features in %d "
            "blocks, %d components; %d diseases with >= %d nearest genes, %d kept "
            "(leave-one-out AUC lower bound > 0.5); median leave-one-out AUC %.3f over all, "
            "%.3f over kept.",
            n_fit,
            len(order) - n_fit,
            n_features,
            blocks[varying].nunique(),
            eigenvalues.size,
            len(diseases),
            self.approx_pops_min_genes,
            int(fit.kept.sum()),
            float(np.nanmedian(fit.auc)),
            float(np.nanmedian(fit.auc[fit.kept])) if fit.kept.any() else float("nan"),
        )
        if not fit.kept.any():
            return empty

        scores = np.vstack([fit.scores, fit.held_out_scores])
        n_genes, n_kept = scores.shape
        with arrow_transfers(spark):
            long_scores = spark.createDataFrame(
                pd.DataFrame(
                    {
                        "geneIndex": np.repeat(np.arange(n_genes), n_kept),
                        "diseaseIndex": np.tile(np.arange(n_kept), n_genes),
                        "approxPops": scores.ravel(),
                    }
                ),
                "geneIndex long, diseaseIndex long, approxPops double",
            )
            gene_names = spark.createDataFrame(
                pd.DataFrame(
                    {"geneIndex": np.arange(n_genes), "geneId": ordered_genes}
                ),
                "geneIndex long, geneId string",
            )
            disease_names = spark.createDataFrame(
                pd.DataFrame(
                    {
                        "diseaseIndex": np.arange(n_kept),
                        "diseaseId": diseases[fit.kept].astype(str),
                    }
                ),
                "diseaseIndex long, diseaseId string",
            )
        return (
            long_scores.join(f.broadcast(gene_names), "geneIndex", "inner")
            .join(f.broadcast(disease_names), "diseaseIndex", "inner")
            .select("geneId", "diseaseId", "approxPops")
        )


@dataclass
class PleiotropyPriorScores:
    """Fitted gene priors the pleiotropy prior features join to their genes.

    Attributes:
        prior (DataFrame | None): `geneId` and `predictedPleiotropyPrior`; None when not fitted
        approx_pops (DataFrame | None): `geneId`, `diseaseId` and `approxPops`; None when not
            fitted
    """

    prior: DataFrame | None = None
    approx_pops: DataFrame | None = None

    @classmethod
    def from_release(
        cls: type[PleiotropyPriorScores],
        inputs: PleiotropyPriorInputs,
        features_list: list[str],
        *,
        study_locus: CredibleSetLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> PleiotropyPriorScores:
        """Fit the priors the requested features need, collecting the gene data once.

        Args:
            inputs (PleiotropyPriorInputs): Gene data and settings of the fits
            features_list (list[str]): Requested L2G features
            study_locus (CredibleSetLocus): All credible sets of the release, the source of the
                targets and labels
            study_index (StudyIndex): Study index, used to resolve a study to its diseases
            variant_index (VariantIndex): Variant index, the source of the distances to genes
            target_index (TargetIndex): Target index

        Returns:
            PleiotropyPriorScores: The fitted priors, None for those no requested feature needs
        """
        requested = set(features_list)
        needs_prior = bool(
            requested
            & {
                PredictedPleiotropyPriorFeature.feature_name,
                PredictedPleiotropyPriorNeighbourhoodFeature.feature_name,
            }
        )
        needs_approx_pops = bool(
            requested
            & {
                ApproxPopsFeature.feature_name,
                ApproxPopsNeighbourhoodFeature.feature_name,
            }
        )
        if not (needs_prior or needs_approx_pops):
            return cls()
        data = inputs.load_gene_data(target_index)
        fit_arguments = (study_locus, study_index, variant_index, target_index)
        return cls(
            prior=inputs.fit_prior(data, *fit_arguments) if needs_prior else None,
            approx_pops=(
                inputs.fit_approx_pops(data, *fit_arguments)
                if needs_approx_pops
                else None
            ),
        )


def protein_coding_genes_in_window(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    variant_index: VariantIndex,
    genomic_window: int,
) -> DataFrame:
    """Protein-coding genes the L2G feature matrix keeps for each credible set.

    The same genes as the `isProteinCoding` feature flags with 1: protein-coding genes whose
    footprint lies within `genomic_window` of any variant of the credible set. Training and
    prediction keep only these rows of the feature matrix, so a gene prior defined on them
    covers every row the model sees, and its neighbourhood version is scaled over the same genes.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        genomic_window (int): Largest distance between a variant and a gene footprint

    Returns:
        DataFrame: Distinct `credibleSetId` and `geneId` pairs
    """
    return (
        is_protein_coding_feature_logic(
            study_loci_to_annotate,
            variant_index=variant_index,
            feature_name="isProteinCoding",
            genomic_window=genomic_window,
        )
        .filter(f.col("isProteinCoding") == 1.0)
        .select("credibleSetId", "geneId")
        .distinct()
    )


def min_max_within_locus(
    local_scores: DataFrame, local_feature_name: str, feature_name: str
) -> DataFrame:
    """Rescale a feature between its lowest and highest value at the same locus.

    The best-scoring gene at a locus gets 1 and the worst 0. A locus with a single scored gene,
    or where every gene scores the same, gives 1 to all of them.

    Args:
        local_scores (DataFrame): `credibleSetId`, `geneId` and the local feature
        local_feature_name (str): Name of the local feature column
        feature_name (str): Name of the rescaled feature

    Returns:
        DataFrame: `credibleSetId`, `geneId` and the rescaled feature
    """
    locus = Window.partitionBy("credibleSetId")
    return (
        local_scores.withColumn("regionalMin", f.min(local_feature_name).over(locus))
        .withColumn("regionalMax", f.max(local_feature_name).over(locus))
        .withColumn(
            feature_name,
            f.when(
                f.col("regionalMax") > f.col("regionalMin"),
                (f.col(local_feature_name) - f.col("regionalMin"))
                / (f.col("regionalMax") - f.col("regionalMin")),
            ).otherwise(f.lit(1.0)),
        )
        .drop("regionalMin", "regionalMax", local_feature_name)
    )


def common_pleiotropy_prior_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pleiotropy_prior_scores: PleiotropyPriorScores,
    variant_index: VariantIndex,
    genomic_window: int,
) -> DataFrame:
    """Attach the predicted pleiotropy prior to every protein-coding gene of a credible set.

    The genes are those the feature matrix keeps, see
    [`protein_coding_genes_in_window`][gentropy.dataset.l2g_features.pleiotropy_prior.protein_coding_genes_in_window].
    The value does not depend on the credible set: a gene gets the same prior at every locus.
    A gene without a prior, such as one missing from the target index, takes the mean prior of
    the scored genes, so that it is an average gene rather than, after the within-locus scaling
    of the neighbourhood feature, the worst gene at its locus.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the feature
        pleiotropy_prior_scores (PleiotropyPriorScores): Fitted priors
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        genomic_window (int): Largest distance between a credible-set variant and a gene
            footprint

    Returns:
        DataFrame: Feature dataset with one row per credible set and scored gene in its window

    Raises:
        ValueError: If the predicted pleiotropy prior was not fitted
    """
    scores = pleiotropy_prior_scores.prior
    if scores is None:
        raise ValueError("The predicted pleiotropy prior was not fitted.")
    average = scores.agg(f.mean("predictedPleiotropyPrior").alias("averagePrior"))
    return (
        protein_coding_genes_in_window(
            study_loci_to_annotate, variant_index, genomic_window
        )
        .join(scores, "geneId", "left")
        .crossJoin(f.broadcast(average))
        .select(
            "credibleSetId",
            "geneId",
            f.coalesce("predictedPleiotropyPrior", "averagePrior").alias(feature_name),
        )
    )


def common_neighbourhood_pleiotropy_prior_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    **kwargs: Any,
) -> DataFrame:
    """Rescale the prior of a gene between the lowest and highest prior at the same locus.

    The best-scoring gene at a locus gets 1 and the worst 0, as in the within-locus scaling
    FLAMES applies to PoPS (Schipper et al. 2025, Nat Genet). A min-max scaling rather than
    the division by the locus maximum used by the other neighbourhood features, because the
    prior is centred and so negative for about half of the genes. A locus with a single scored
    gene, or where every gene scores the same, gives 1 to all of them.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the neighbourhood feature, ending in "Neighbourhood"
        **kwargs (Any): Arguments of `common_pleiotropy_prior_feature_logic`

    Returns:
        DataFrame: Feature dataset with one row per credible set and scored gene in its window
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_scores = common_pleiotropy_prior_feature_logic(
        study_loci_to_annotate, local_feature_name, **kwargs
    )
    return min_max_within_locus(local_scores, local_feature_name, feature_name)


def common_approx_pops_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pleiotropy_prior_scores: PleiotropyPriorScores,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    genomic_window: int,
) -> DataFrame:
    """Attach to every protein-coding gene of a credible set its approxPops score for the study.

    The genes are those the feature matrix keeps, see
    [`protein_coding_genes_in_window`][gentropy.dataset.l2g_features.pleiotropy_prior.protein_coding_genes_in_window].
    A study mapped to several diseases takes the highest score of the gene over them. A credible
    set whose study has no disease with an approxPops score gets no rows. At the other credible
    sets, a gene without a score takes 0, the score of an average gene on the z-score scale, so
    that it is not, after the within-locus scaling of the neighbourhood feature, the worst gene
    at its locus.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the feature
        pleiotropy_prior_scores (PleiotropyPriorScores): Fitted priors
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        genomic_window (int): Largest distance between a credible-set variant and a gene
            footprint

    Returns:
        DataFrame: Feature dataset with one row per credible set and scored gene in its window

    Raises:
        ValueError: If approxPops was not fitted
    """
    scores = pleiotropy_prior_scores.approx_pops
    if scores is None:
        raise ValueError("approxPops was not fitted.")
    study_diseases = (
        study_loci_to_annotate.df.select("credibleSetId", "studyId")
        .join(
            study_index.df.select(
                "studyId", f.explode("diseaseIds").alias("diseaseId")
            ),
            "studyId",
            "inner",
        )
        .join(scores.select("diseaseId").distinct(), "diseaseId", "semi")
        .select("credibleSetId", "diseaseId")
    )
    return (
        protein_coding_genes_in_window(
            study_loci_to_annotate, variant_index, genomic_window
        )
        .join(study_diseases, "credibleSetId", "inner")
        .join(scores, ["geneId", "diseaseId"], "left")
        .groupBy("credibleSetId", "geneId")
        .agg(f.coalesce(f.max("approxPops"), f.lit(0.0)).alias(feature_name))
    )


def common_neighbourhood_approx_pops_feature_logic(
    study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
    feature_name: str,
    **kwargs: Any,
) -> DataFrame:
    """Rescale the approxPops score of a gene between the lowest and highest at the same locus.

    Args:
        study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the neighbourhood feature, ending in "Neighbourhood"
        **kwargs (Any): Arguments of `common_approx_pops_feature_logic`

    Returns:
        DataFrame: Feature dataset with one row per credible set and scored gene in its window
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_scores = common_approx_pops_feature_logic(
        study_loci_to_annotate, local_feature_name, **kwargs
    )
    return min_max_within_locus(local_scores, local_feature_name, feature_name)


class PredictedPleiotropyPriorFeature(L2GFeature):
    """How much a gene looks, by its Open Targets gene data, like the nearest genes of GWAS loci."""

    feature_dependency_type = [PleiotropyPriorScores, VariantIndex]
    feature_name = "predictedPleiotropyPrior"
    genomic_window: int = GENE_FOOTPRINT_WINDOW

    @classmethod
    def compute(
        cls: type[PredictedPleiotropyPriorFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PredictedPleiotropyPriorFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The fitted priors and the variant index

        Returns:
            PredictedPleiotropyPriorFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_pleiotropy_prior_feature_logic(
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


class PredictedPleiotropyPriorNeighbourhoodFeature(L2GFeature):
    """Predicted pleiotropy prior of a gene rescaled between the lowest and highest at the locus."""

    feature_dependency_type = [PleiotropyPriorScores, VariantIndex]
    feature_name = "predictedPleiotropyPriorNeighbourhood"
    genomic_window: int = GENE_FOOTPRINT_WINDOW

    @classmethod
    def compute(
        cls: type[PredictedPleiotropyPriorNeighbourhoodFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PredictedPleiotropyPriorNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The fitted priors and the variant index

        Returns:
            PredictedPleiotropyPriorNeighbourhoodFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_pleiotropy_prior_feature_logic(
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


class ApproxPopsFeature(L2GFeature):
    """How much a gene looks, by its Open Targets gene data, like the nearest genes of the study's disease."""

    feature_dependency_type = [PleiotropyPriorScores, StudyIndex, VariantIndex]
    feature_name = "approxPops"
    genomic_window: int = GENE_FOOTPRINT_WINDOW

    @classmethod
    def compute(
        cls: type[ApproxPopsFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> ApproxPopsFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The fitted priors, the study index and the variant index

        Returns:
            ApproxPopsFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_approx_pops_feature_logic(
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


class ApproxPopsNeighbourhoodFeature(L2GFeature):
    """approxPops score of a gene rescaled between the lowest and highest at the locus."""

    feature_dependency_type = [PleiotropyPriorScores, StudyIndex, VariantIndex]
    feature_name = "approxPopsNeighbourhood"
    genomic_window: int = GENE_FOOTPRINT_WINDOW

    @classmethod
    def compute(
        cls: type[ApproxPopsNeighbourhoodFeature],
        study_loci_to_annotate: CredibleSetLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> ApproxPopsNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (CredibleSetLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The fitted priors, the study index and the variant index

        Returns:
            ApproxPopsNeighbourhoodFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_approx_pops_feature_logic(
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
