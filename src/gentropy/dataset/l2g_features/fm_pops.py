"""Methods to generate features from the fine-mapping-based polygenic priority score (fmPops)."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.l2g_features.distance import (
    DistanceSentinelTssNeighbourhoodFeature,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.external.gcs import copy_from_gcs
from gentropy.method.fm_pops import FmPops

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

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
    every tied gene. Only GWAS credible sets count, and a disease counts once per gene however
    many credible sets or studies point at it.

    TODO: related traits are counted as separate diseases, which inflates the counts of genes
    at loci shared by many lipid or blood-cell traits. Grouping diseases through the EFO
    hierarchy or by therapeutic area would temper that.

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


def gene_covariates(
    target_index: TargetIndex, genomic_window: int = 500_000
) -> DataFrame:
    """Gene-level covariates of the fmPops target, for the protein-coding genes of a release.

    A gene in a gene desert is the nearest gene to every locus around it, so the number of
    diseases it is nearest for depends on its surroundings as much as on its biology. Two
    covariates capture that and are projected out of the target: the log of the gene length and
    the log of one plus the number of other protein-coding genes with a TSS within
    `genomic_window` of the gene's TSS.

    Args:
        target_index (TargetIndex): Target index
        genomic_window (int): Distance from the TSS within which neighbouring genes count

    Returns:
        DataFrame: `geneId`, `chromosome`, `tss`, `logGeneLength` and `logNeighbouringGenes`,
            one row per protein-coding gene with a TSS
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


class PopsGeneFeatures:
    """The PoPS gene features, and the fmPops gene scores fitted on them.

    fmPops keeps the model of the polygenic priority score (PoPS; Weeks et al. 2023, Nat Genet
    55:1267-1276) and replaces its MAGMA target, which needs full summary statistics, with one
    computed from credible sets: for every protein-coding gene, the log of one plus the number
    of distinct diseases it is the nearest gene for across all GWAS credible sets. The target
    is regressed on the PoPS gene features with a leave-one-chromosome-out kernel ridge
    regression, see [`FmPops`][gentropy.method.fm_pops.FmPops]. No label from the L2G gold
    standard enters the score.

    Before fitting, an intercept, the 84 PoPS control features and two covariates from the
    target index (log gene length and the log number of protein-coding genes within 500 kb) are
    projected out of the target within each fold. The control features do not enter the
    kernel. Genes in the major histocompatibility complex are scored but kept out of every
    training fold: credible sets with a lead variant in the region are flagged and usually
    excluded from a release, so these genes are rarely anyone's nearest gene for a reason that
    has nothing to do with their biology.

    The scores are computed once, the first time a feature asks for them, and kept for the
    other fmPops features of the same run. The fit runs on the Spark driver: the kernel over
    about 18,000 genes takes 2.7 GB and each fold holds a copy of its training block and its
    eigenvectors on top, so peak memory is about 10 GB. One fold per chromosome, 22 in all,
    takes tens of minutes in total.

    Inputs. `feature_dir` is the `pops_features_full` directory of the FLAMES annotation data
    (Schipper, Zenodo 10.5281/zenodo.12635505, CC BY 4.0): `control.features`,
    `munged_features/pops_features.rows.txt` and the `pops_features.cols.{i}.txt` and
    `pops_features.mat.{i}.npy` chunks. Only genes that are protein coding in the target index
    and present in the feature rows are scored. A `gs://` directory is copied to local disk
    first (about 9 GB).

    Licence. Cite Weeks et al. 2023 and the Zenodo record when using the features. The
    features derive from sources with their own terms: the pathway features include KEGG,
    which restricts commercial use, and the protein-protein interaction features come from
    InWeb_IM, whose commercial terms need checking. The pathway columns are anonymous, so KEGG
    cannot be filtered out. A licence review is needed before fmPops is used in a release.
    """

    def __init__(
        self: PopsGeneFeatures,
        feature_dir: str,
        lambda_grid: list[float] | None = None,
    ) -> None:
        """Point at the PoPS gene features.

        Args:
            feature_dir (str): Directory of the extracted PoPS gene features, local or on GCS
            lambda_grid (list[float] | None): Ridge penalties searched by generalised
                cross-validation. Defaults to `10^-2` to `10^10` in steps of `10^0.5`.
        """
        self.feature_dir = feature_dir
        self.lambda_grid = lambda_grid
        self._scores: DataFrame | None = None

    def scores(
        self: PopsGeneFeatures,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Return the fmPops score of every gene, fitted on the first call and reused after.

        Args:
            study_locus (StudyLocus): All credible sets of the release, the source of the target
            study_index (StudyIndex): Study index, used to resolve a study to its diseases
            variant_index (VariantIndex): Variant index, the source of the distances to genes
            target_index (TargetIndex): Target index

        Returns:
            DataFrame: `geneId` and `fmPops`
        """
        if self._scores is None:
            self._scores = self._fit(
                study_locus, study_index, variant_index, target_index
            )
        return self._scores

    def _fit(
        self: PopsGeneFeatures,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> DataFrame:
        """Collect the target and covariates, fit fmPops on the driver and return the scores.

        Args:
            study_locus (StudyLocus): All credible sets of the release
            study_index (StudyIndex): Study index
            variant_index (VariantIndex): Variant index
            target_index (TargetIndex): Target index

        Returns:
            DataFrame: `geneId` and `fmPops`
        """
        counts = nearest_gene_disease_counts(
            study_locus, study_index, variant_index, target_index
        )
        genes = (
            gene_covariates(target_index)
            .join(counts, "geneId", "left")
            .withColumn("targetCount", f.coalesce(f.col("targetCount"), f.lit(0)))
            .toPandas()
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            feature_dir = self.feature_dir
            if feature_dir.startswith("gs://"):
                copy_from_gcs(feature_dir, tmp_dir)
                feature_dir = tmp_dir

            feature_genes = set(FmPops.read_feature_genes(feature_dir))
            genes = (
                genes[genes["geneId"].isin(feature_genes)]
                .sort_values("geneId")
                .reset_index(drop=True)
            )
            gene_ids = genes["geneId"].tolist()
            control_names = FmPops.read_control_feature_names(feature_dir)
            control_columns: list[np.ndarray] = []

            def non_control_chunks() -> Iterator[np.ndarray]:
                """Yield the feature chunks without their control columns, keeping those aside.

                Yields:
                    np.ndarray: Feature chunk without control columns
                """
                for columns, chunk in FmPops.read_feature_chunks(feature_dir, gene_ids):
                    is_control = np.array([c in control_names for c in columns])
                    if is_control.any():
                        control_columns.append(chunk[:, is_control])
                    yield chunk[:, ~is_control]

            kernel, n_features = FmPops.accumulate_kernel(non_control_chunks())

        controls = (
            FmPops.standardise(np.column_stack(control_columns))
            if control_columns
            else np.empty((len(gene_ids), 0))
        )
        covariates = np.column_stack(
            [controls, genes[["logGeneLength", "logNeighbouringGenes"]].to_numpy()]
        )
        mhc = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)
        in_mhc = (
            (genes["chromosome"] == mhc.chromosome)
            & genes["tss"].between(mhc.start, mhc.end)
        ).to_numpy()

        fit = FmPops.loco_kernel_ridge(
            kernel,
            np.log1p(genes["targetCount"].to_numpy(dtype=np.float64)),
            genes["chromosome"].to_numpy(dtype=str),
            covariates=covariates,
            fit_mask=~in_mhc,
            lambdas=self.lambda_grid or FmPops.DEFAULT_LAMBDA_GRID,
        )
        logger.info(
            "fmPops: %d genes, %d features, %d control features, %d genes nearest to at "
            "least one disease, %d MHC genes kept out of the fit.",
            len(gene_ids),
            n_features,
            controls.shape[1],
            int((genes["targetCount"] > 0).sum()),
            int(in_mhc.sum()),
        )
        for held_out, penalty in fit.lambdas.items():
            logger.info("fmPops: chromosome %s, lambda %.3g", held_out, penalty)

        genes["fmPops"] = fit.scores
        return study_locus.df.sparkSession.createDataFrame(
            genes[["geneId", "fmPops"]].astype({"fmPops": "float64"}),
            schema="geneId string, fmPops double",
        )


def common_fm_pops_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    *,
    pops_gene_features: PopsGeneFeatures,
    study_locus: StudyLocus,
    study_index: StudyIndex,
    variant_index: VariantIndex,
    target_index: TargetIndex,
    genomic_window: int,
) -> DataFrame:
    """Attach the gene-level fmPops prior to every protein-coding gene near a credible set.

    The value does not depend on the credible set: a gene gets the same `fmPops` at every
    locus. Genes without a score, mostly protein-coding genes the PoPS features do not cover,
    get no row and are filled with 0 in the feature matrix, which is the score of an average
    gene because the target is centred.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the feature
        pops_gene_features (PopsGeneFeatures): PoPS gene features the scores are fitted on
        study_locus (StudyLocus): All credible sets, the source of the fmPops target and of the
            position of the study locus
        study_index (StudyIndex): Study index, used to resolve a study to its diseases
        variant_index (VariantIndex): Variant index, the source of the distances to genes
        target_index (TargetIndex): Target index, used for gene positions and biotypes
        genomic_window (int): Distance up and downstream of the study locus to collect genes from

    Returns:
        DataFrame: Feature dataset with one row per study locus and scored gene in its window
    """
    scores = pops_gene_features.scores(
        study_locus, study_index, variant_index, target_index
    )
    genes_in_window = (
        study_locus.df.select("studyLocusId", "chromosome", "position")
        .join(
            study_loci_to_annotate.df.select("studyLocusId").distinct(),
            "studyLocusId",
            "semi",
        )
        .join(
            target_index.df.filter(f.col("biotype") == "protein_coding").select(
                f.col("id").alias("geneId"),
                f.col("genomicLocation.chromosome").alias("geneChromosome"),
                "tss",
            ),
            on=(f.col("chromosome") == f.col("geneChromosome"))
            & (f.abs(f.col("tss") - f.col("position")) <= genomic_window),
            how="inner",
        )
        .select("studyLocusId", "geneId")
        .distinct()
    )
    return genes_in_window.join(
        scores.select("geneId", f.col("fmPops").alias(feature_name)),
        "geneId",
        "inner",
    ).select("studyLocusId", "geneId", feature_name)


def common_neighbourhood_fm_pops_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    **kwargs: Any,
) -> DataFrame:
    """Rescale the fmPops prior of a gene between the lowest and highest prior at the same locus.

    The best-scoring gene at a locus gets 1 and the worst 0, as in the within-locus scaling
    FLAMES applies to PoPS (Schipper et al. 2025, Nat Genet). A min-max scaling rather than
    the division by the locus maximum used by the other neighbourhood features, because
    `fmPops` is centred and so negative for about half of the genes. A locus with a single
    scored gene, or where every gene scores the same, gives 1 to all of them.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_name (str): The name of the neighbourhood feature, ending in "Neighbourhood"
        **kwargs (Any): Arguments of `common_fm_pops_feature_logic`

    Returns:
        DataFrame: Feature dataset with one row per study locus and scored gene in its window
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_scores = common_fm_pops_feature_logic(
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


class FmPopsFeature(L2GFeature):
    """Gene-level prior predicted from gene features, learnt from the nearest genes of GWAS loci."""

    feature_dependency_type = [
        PopsGeneFeatures,
        StudyLocus,
        StudyIndex,
        VariantIndex,
        TargetIndex,
    ]
    feature_name = "fmPops"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[FmPopsFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> FmPopsFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The PoPS gene features, the credible sets, studies, variants and genes

        Returns:
            FmPopsFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_fm_pops_feature_logic(
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


class FmPopsNeighbourhoodFeature(L2GFeature):
    """fmPops prior of a gene rescaled between the lowest and highest prior at the same locus."""

    feature_dependency_type = [
        PopsGeneFeatures,
        StudyLocus,
        StudyIndex,
        VariantIndex,
        TargetIndex,
    ]
    feature_name = "fmPopsNeighbourhood"
    genomic_window: int = 500_000

    @classmethod
    def compute(
        cls: type[FmPopsNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> FmPopsNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): The PoPS gene features, the credible sets, studies, variants and genes

        Returns:
            FmPopsNeighbourhoodFeature: Feature dataset
        """
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_fm_pops_feature_logic(
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
