"""Methods to generate the predicted pleiotropy prior features from Open Targets gene data."""

from __future__ import annotations

import logging
from functools import reduce
from typing import Any

import numpy as np
import pyspark.sql.functions as f
import scipy.sparse
from pyspark.sql import DataFrame, Window

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.l2g_features.distance import (
    DistanceSentinelTssNeighbourhoodFeature,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_features.other import is_protein_coding_feature_logic
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.pleiotropy_prior import PleiotropyPrior

logger = logging.getLogger(__name__)


def nearest_gene_disease_counts(
    study_locus: StudyLocus,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    target_index: TargetIndex,
) -> DataFrame:
    """Count the distinct diseases each gene is the nearest gene for.

    The nearest gene of a credible set is the protein-coding gene whose TSS is closest to the
    lead variant, i.e. the gene with `distanceSentinelTssNeighbourhood` equal to 1, so ties keep
    every tied gene. Only GWAS credible sets count. Diseases are the ontology terms in the
    `diseaseIds` field of the study index, used as mapped, with no expansion to ancestors; a
    study mapped to several terms contributes each of them, and a term counts once per gene
    however many credible sets or studies point at it.

    TODO: related traits mapped to different terms are counted separately, which inflates the
    counts of genes at loci shared by many lipid or blood-cell traits. Grouping terms through
    the EFO hierarchy or by therapeutic area would temper that.

    Args:
        study_locus (StudyLocus): Credible sets
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        target_index (TargetIndex): Target index, used to keep protein-coding genes

    Returns:
        DataFrame: `geneId` and `targetCount`, for the genes nearest to at least one credible
            set of a study with a disease
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


def gene_covariates(target_index: TargetIndex, genomic_window: int = 500_000) -> DataFrame:
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
    genes = target_index.df.filter(
        (f.col("biotype") == "protein_coding")
        & f.col("tss").isNotNull()
        & f.col("genomicLocation.chromosome").isNotNull()
    ).select(
        f.col("id").alias("geneId"),
        f.col("genomicLocation.chromosome").alias("chromosome"),
        "tss",
        f.log1p(
            f.coalesce(
                f.abs(f.col("genomicLocation.end") - f.col("genomicLocation.start")),
                f.lit(0),
            )
        ).alias("logGeneLength"),
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


class PleiotropyPriorInputs:
    """Open Targets gene data the predicted pleiotropy prior is fitted on, and the fitted prior.

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
    | PPI              | `interaction`, STRING >= 0.7, IntAct >= 0.45 | first-degree neighbours of each hub gene     |
    | Mouse phenotypes | `mouse_phenotype`                          | membership of each phenotype term                |
    | Expression       | `baseline_expression`                      | level and within-source specificity per context |
    | Constraint       | `target.constraint`                        | LoF oe, oeUpper, score; missense oe, score; synonymous score |
    | Essentiality     | `target_essentiality.isEssential`          | 0/1                                              |

    Binary blocks keep terms with 10 to 2,000 member genes, counted among protein-coding
    genes. Missing constraint values take the mean of the genes that have one. Every column is
    standardised across genes and columns without variance are dropped; there is no block
    weighting and no feature selection.

    The target is regressed on the features with the leave-one-chromosome-out kernel ridge
    regression of [`PleiotropyPrior`][gentropy.method.pleiotropy_prior.PleiotropyPrior], with an
    intercept, the two positional covariates of
    [`gene_covariates`][gentropy.dataset.l2g_features.pleiotropy_prior.gene_covariates] and one
    0/1 coverage flag per source projected out within each fold. Genes in the major
    histocompatibility complex are scored but kept out of every training fold: credible sets
    with a lead variant in the region are flagged and usually excluded from a release, so these
    genes are rarely anyone's nearest gene for a reason that has nothing to do with their
    biology. No L2G score and no gold-standard label enters the prior.

    The prior is fitted once, the first time a feature asks for it, and kept for the other
    feature of the same run. The fit runs on the Spark driver: the kernel over about 20,000
    genes takes 3.2 GB and each fold holds a copy of its training block and its eigenvectors on
    top, so peak memory is about 12 GB.
    """

    MIN_TERM_SIZE = 10
    MAX_TERM_SIZE = 2_000
    STRING_MIN_SCORE = 0.7
    INTACT_MIN_SCORE = 0.45
    EXPRESSION_SOURCES = ("gtex", "tabula_sapiens", "dice", "pride")
    CONSTRAINT_FEATURES = (
        ("lof", "oe"),
        ("lof", "oeUpper"),
        ("lof", "score"),
        ("mis", "oe"),
        ("mis", "score"),
        ("syn", "score"),
    )

    def __init__(
        self: PleiotropyPriorInputs,
        interactions: DataFrame,
        mouse_phenotype: DataFrame,
        baseline_expression: DataFrame,
        target_essentiality: DataFrame,
        lambda_grid: list[float] | None = None,
    ) -> None:
        """Hold the gene data of a release.

        Args:
            interactions (DataFrame): Open Targets `interaction` dataset
            mouse_phenotype (DataFrame): Open Targets `mouse_phenotype` dataset
            baseline_expression (DataFrame): Open Targets `baseline_expression` dataset
            target_essentiality (DataFrame): Open Targets `target_essentiality` dataset
            lambda_grid (list[float] | None): Ridge penalties searched by generalised
                cross-validation. Defaults to `10^-2` to `10^10` in steps of `10^0.5`.
        """
        self.interactions = interactions
        self.mouse_phenotype = mouse_phenotype
        self.baseline_expression = baseline_expression
        self.target_essentiality = target_essentiality
        self.lambda_grid = lambda_grid
        self._scores: DataFrame | None = None

    def gene_features(
        self: PleiotropyPriorInputs, genes: DataFrame, target_index: TargetIndex
    ) -> tuple[DataFrame, DataFrame]:
        """Build the gene features and the per-source coverage of the background genes.

        Args:
            genes (DataFrame): Background genes, with a `geneId` column
            target_index (TargetIndex): Target index, the source of GO, Reactome and constraint

        Returns:
            tuple[DataFrame, DataFrame]: Features as `geneId`, `featureId`, `value` rows, and
                coverage as distinct `geneId`, `source` pairs
        """
        background = genes.select("geneId")
        targets = target_index.df.withColumnRenamed("id", "geneId").join(
            background, "geneId", "semi"
        )

        memberships = (
            targets.select(
                "geneId",
                f.lit("go").alias("source"),
                f.explode("go.id").alias("term"),
            )
            .unionByName(
                targets.select(
                    "geneId",
                    f.lit("reactome").alias("source"),
                    f.explode("pathways.pathwayId").alias("term"),
                )
            )
            .unionByName(self._ppi_neighbours(background))
            .unionByName(
                self.mouse_phenotype.select(
                    f.col("targetFromSourceId").alias("geneId"),
                    f.lit("mouse_phenotype").alias("source"),
                    f.col("modelPhenotypeId").alias("term"),
                ).join(background, "geneId", "semi")
            )
            .filter(f.col("term").isNotNull())
            .distinct()
        )
        term_size = Window.partitionBy("source", "term")
        binary = (
            memberships.withColumn("termSize", f.count("*").over(term_size))
            .filter(
                f.col("termSize").between(self.MIN_TERM_SIZE, self.MAX_TERM_SIZE)
            )
            .select(
                "geneId",
                f.concat_ws(":", "source", "term").alias("featureId"),
                f.lit(1.0).alias("value"),
            )
        )

        expression = self._expression(background)
        constraint = self._constraint(targets, background)
        essentiality = self.target_essentiality.select(
            f.col("targetId").alias("geneId"),
            f.lit("essentiality:isEssential").alias("featureId"),
            f.col("isEssential").cast("double").alias("value"),
        ).join(background, "geneId", "semi")

        features = binary.unionByName(
            expression.select("geneId", "featureId", "value")
        ).unionByName(constraint).unionByName(essentiality)
        coverage = (
            memberships.select("geneId", "source")
            .unionByName(
                expression.select(
                    "geneId",
                    f.concat_ws(":", f.lit("expression"), "source").alias("source"),
                )
            )
            .unionByName(
                targets.filter(f.size("constraint") > 0).select(
                    "geneId", f.lit("constraint").alias("source")
                )
            )
            .unionByName(
                essentiality.select("geneId", f.lit("essentiality").alias("source"))
            )
            .distinct()
        )
        return features, coverage

    def _ppi_neighbours(self: PleiotropyPriorInputs, background: DataFrame) -> DataFrame:
        """First-degree neighbours of every hub gene in the high-confidence interaction network.

        Args:
            background (DataFrame): Background genes, with a `geneId` column

        Returns:
            DataFrame: `geneId` (the neighbour), `source` and `term` (the hub)
        """
        edges = self.interactions.filter(
            (
                (f.col("sourceDatabase") == "string")
                & (f.col("scoring") >= self.STRING_MIN_SCORE)
            )
            | (
                (f.col("sourceDatabase") == "intact")
                & (f.col("scoring") >= self.INTACT_MIN_SCORE)
            )
        ).filter(f.col("targetA") != f.col("targetB"))
        undirected = edges.select(
            f.col("targetA").alias("hub"), f.col("targetB").alias("geneId")
        ).unionByName(
            edges.select(f.col("targetB").alias("hub"), f.col("targetA").alias("geneId"))
        )
        return (
            undirected.join(background, "geneId", "semi")
            .join(background.withColumnRenamed("geneId", "hub"), "hub", "semi")
            .select("geneId", f.lit("ppi").alias("source"), f.col("hub").alias("term"))
        )

    def _expression(self: PleiotropyPriorInputs, background: DataFrame) -> DataFrame:
        """Expression level and within-source specificity of every gene in every context.

        A context is a tissue and cell type pair of one source. The level is
        `log(1 + max(median, 0))`, averaged over repeated measurements of the context. The
        specificity is the level z-scored across the contexts of the same source in which the
        gene is measured, and 0 when the gene's levels do not vary.

        Args:
            background (DataFrame): Background genes, with a `geneId` column

        Returns:
            DataFrame: `geneId`, `source`, `featureId` and `value`
        """
        levels = (
            self.baseline_expression.withColumn(
                "source", f.lower(f.col("datasourceId"))
            )
            .filter(f.col("source").isin(*self.EXPRESSION_SOURCES))
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
            f.col("level").alias("value"),
        ).unionByName(
            levels.select(
                "geneId",
                "source",
                f.concat_ws(":", "source", f.lit("specificity"), "context").alias(
                    "featureId"
                ),
                specificity.alias("value"),
            )
        )

    def _constraint(
        self: PleiotropyPriorInputs, targets: DataFrame, background: DataFrame
    ) -> DataFrame:
        """Constraint metrics of every background gene, missing values set to the mean.

        Args:
            targets (DataFrame): Target index rows of the background genes, `id` as `geneId`
            background (DataFrame): Background genes, with a `geneId` column

        Returns:
            DataFrame: `geneId`, `featureId` and `value`
        """
        constraints = targets.select(
            "geneId", f.explode("constraint").alias("constraint")
        ).select("geneId", "constraint.*")
        observed = reduce(
            DataFrame.unionByName,
            [
                constraints.filter(f.col("constraintType") == constraint_type).select(
                    "geneId",
                    f.lit(f"constraint:{constraint_type}:{field}").alias("featureId"),
                    f.col(field).cast("double").alias("observed"),
                )
                for constraint_type, field in self.CONSTRAINT_FEATURES
            ],
        )
        feature_ids = background.sparkSession.createDataFrame(
            [
                (f"constraint:{constraint_type}:{field}",)
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
                f.coalesce(
                    f.col("observed"),
                    f.mean("observed").over(Window.partitionBy("featureId")),
                ).alias("value"),
            )
        )

    def scores(
        self: PleiotropyPriorInputs,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Return the prior of every background gene, fitted on the first call and reused after.

        Args:
            study_locus (StudyLocus): All credible sets of the release, the source of the target
            study_index (StudyIndex): Study index, used to resolve a study to its diseases
            variant_index (VariantIndex): Variant index, the source of the distances to genes
            target_index (TargetIndex): Target index

        Returns:
            DataFrame: `geneId` and `predictedPleiotropyPrior`
        """
        if self._scores is None:
            self._scores = self._fit(study_locus, study_index, variant_index, target_index)
        return self._scores

    def _fit(
        self: PleiotropyPriorInputs,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Collect the target, features and covariates, fit the prior on the driver.

        Args:
            study_locus (StudyLocus): All credible sets of the release
            study_index (StudyIndex): Study index
            variant_index (VariantIndex): Variant index
            target_index (TargetIndex): Target index

        Returns:
            DataFrame: `geneId` and `predictedPleiotropyPrior`
        """
        spark = study_locus.df.sparkSession
        counts = nearest_gene_disease_counts(
            study_locus, study_index, variant_index, target_index
        )
        genes = (
            gene_covariates(target_index)
            .join(counts, "geneId", "left")
            .withColumn("targetCount", f.coalesce(f.col("targetCount"), f.lit(0)))
            .toPandas()
            .sort_values("geneId")
            .reset_index(drop=True)
        )
        gene_index = spark.createDataFrame(
            genes[["geneId"]].assign(geneIndex=np.arange(len(genes), dtype=np.int64))
        )

        features, coverage = self.gene_features(
            spark.createDataFrame(genes[["geneId"]]), target_index
        )
        feature_ids = sorted(
            row["featureId"] for row in features.select("featureId").distinct().collect()
        )
        feature_index = spark.createDataFrame(
            [(feature_id, i) for i, feature_id in enumerate(feature_ids)],
            "featureId string, featureIndex long",
        )
        entries = (
            features.join(gene_index, "geneId", "inner")
            .join(feature_index, "featureId", "inner")
            .select("geneIndex", "featureIndex", "value")
            .toPandas()
        )
        matrix = scipy.sparse.csc_array(
            (
                entries["value"].to_numpy(dtype=np.float64),
                (
                    entries["geneIndex"].to_numpy(dtype=np.int64),
                    entries["featureIndex"].to_numpy(dtype=np.int64),
                ),
            ),
            shape=(len(genes), len(feature_ids)),
        )
        kernel, n_features = PleiotropyPrior.accumulate_kernel(
            PleiotropyPrior.column_chunks(matrix)
        )

        flags = (
            coverage.toPandas()
            .assign(covered=1.0)
            .pivot_table(index="geneId", columns="source", values="covered", fill_value=0.0)
            .reindex(genes["geneId"], fill_value=0.0)
        )
        covariates = np.column_stack(
            [
                genes[["logGeneLength", "logNeighbouringGenes"]].to_numpy(),
                flags.to_numpy(dtype=np.float64),
            ]
        )
        mhc = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)
        in_mhc = (
            (genes["chromosome"] == mhc.chromosome)
            & genes["tss"].between(mhc.start, mhc.end)
        ).to_numpy()

        fit = PleiotropyPrior.loco_kernel_ridge(
            kernel,
            np.log1p(genes["targetCount"].to_numpy(dtype=np.float64)),
            genes["chromosome"].to_numpy(dtype=str),
            covariates=covariates,
            fit_mask=~in_mhc,
            lambdas=self.lambda_grid or PleiotropyPrior.DEFAULT_LAMBDA_GRID,
        )
        logger.info(
            "Predicted pleiotropy prior: %d genes, %d features (%d non-constant), "
            "coverage flags for %s, %d genes nearest to at least one disease, %d MHC genes "
            "kept out of the fit.",
            len(genes),
            len(feature_ids),
            n_features,
            ", ".join(flags.columns),
            int((genes["targetCount"] > 0).sum()),
            int(in_mhc.sum()),
        )
        for held_out, penalty in fit.lambdas.items():
            logger.info(
                "Predicted pleiotropy prior: chromosome %s, lambda %.3g",
                held_out,
                penalty,
            )

        return spark.createDataFrame(
            genes[["geneId"]].assign(predictedPleiotropyPrior=fit.scores),
            schema="geneId string, predictedPleiotropyPrior double",
        )


def protein_coding_genes_in_window(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    variant_index: VariantIndex,
    genomic_window: int,
) -> DataFrame:
    """Protein-coding genes the L2G feature matrix keeps for each credible set.

    The same genes as the `isProteinCoding` feature flags with 1: protein-coding genes whose
    footprint lies within `genomic_window` of any variant of the credible set. Training and
    prediction keep only these rows of the feature matrix, so a gene prior defined on them
    covers every row the model sees, and its neighbourhood version is scaled over the same genes.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        genomic_window (int): Largest distance between a variant and a gene footprint

    Returns:
        DataFrame: Distinct `studyLocusId` and `geneId` pairs
    """
    return (
        is_protein_coding_feature_logic(
            study_loci_to_annotate,
            variant_index=variant_index,
            feature_name="isProteinCoding",
            genomic_window=genomic_window,
        )
        .filter(f.col("isProteinCoding") == 1.0)
        .select("studyLocusId", "geneId")
        .distinct()
    )


def common_pleiotropy_prior_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pleiotropy_prior_inputs: PleiotropyPriorInputs,
    study_locus: StudyLocus,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    target_index: TargetIndex,
    genomic_window: int,
) -> DataFrame:
    """Attach the predicted pleiotropy prior to every protein-coding gene of a credible set.

    The genes are those the feature matrix keeps, see
    [`protein_coding_genes_in_window`][gentropy.dataset.l2g_features.pleiotropy_prior.protein_coding_genes_in_window].
    The value does not depend on the credible set: a gene gets the same prior at every locus.
    Genes without a prior get no row and are filled with 0 in the feature matrix, which is the
    prior of an average gene because the target is centred.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the feature
        pleiotropy_prior_inputs (PleiotropyPriorInputs): Gene data the prior is fitted on
        study_locus (StudyLocus): All credible sets, the source of the target
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        target_index (TargetIndex): Target index
        genomic_window (int): Largest distance between a credible-set variant and a gene
            footprint

    Returns:
        DataFrame: Feature dataset with one row per study locus and scored gene in its window
    """
    scores = pleiotropy_prior_inputs.scores(
        study_locus, study_index, variant_index, target_index
    )
    return protein_coding_genes_in_window(
        study_loci_to_annotate, variant_index, genomic_window
    ).join(
        scores.select(
            "geneId", f.col("predictedPleiotropyPrior").alias(feature_name)
        ),
        "geneId",
        "inner",
    ).select("studyLocusId", "geneId", feature_name)


def common_neighbourhood_pleiotropy_prior_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
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
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the neighbourhood feature, ending in "Neighbourhood"
        **kwargs (Any): Arguments of `common_pleiotropy_prior_feature_logic`

    Returns:
        DataFrame: Feature dataset with one row per study locus and scored gene in its window
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_scores = common_pleiotropy_prior_feature_logic(
        study_loci_to_annotate, local_feature_name, **kwargs
    )
    locus = Window.partitionBy("studyLocusId")
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


class PredictedPleiotropyPriorFeature(L2GFeature):
    """How much a gene looks, by its Open Targets gene data, like the nearest genes of GWAS loci."""

    feature_dependency_type = [
        PleiotropyPriorInputs,
        StudyLocus,
        StudyIndex,
        VariantIndex,
        TargetIndex,
    ]
    feature_name = "predictedPleiotropyPrior"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PredictedPleiotropyPriorFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PredictedPleiotropyPriorFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The gene data of the prior, the credible sets, studies, variants and genes

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
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PredictedPleiotropyPriorNeighbourhoodFeature(L2GFeature):
    """Predicted pleiotropy prior of a gene rescaled between the lowest and highest at the locus."""

    feature_dependency_type = [
        PleiotropyPriorInputs,
        StudyLocus,
        StudyIndex,
        VariantIndex,
        TargetIndex,
    ]
    feature_name = "predictedPleiotropyPriorNeighbourhood"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[PredictedPleiotropyPriorNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PredictedPleiotropyPriorNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The gene data of the prior, the credible sets, studies, variants and genes

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
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
