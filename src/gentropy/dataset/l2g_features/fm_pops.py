"""Methods to generate features from the fine-mapping-based polygenic priority score."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.fm_pops_score import FmPopsScore
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def common_fm_pops_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    feature_name: str,
    *,
    fm_pops_score: FmPopsScore,
    study_locus: StudyLocus,
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
        fm_pops_score (FmPopsScore): Gene-level fmPops scores
        study_locus (StudyLocus): Credible sets, used for the position of the study locus
        target_index (TargetIndex): Target index, used for gene positions and biotypes
        genomic_window (int): Distance up and downstream of the study locus to collect genes from

    Returns:
        DataFrame: Feature dataset with one row per study locus and scored gene in its window
    """
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
        fm_pops_score.df.filter(f.col("fmPops").isNotNull()).select(
            "geneId", f.col("fmPops").alias(feature_name)
        ),
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

    feature_dependency_type = [FmPopsScore, StudyLocus, TargetIndex]
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
            feature_dependency (dict[str, Any]): Datasets with the fmPops scores, the credible sets and the genes

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

    feature_dependency_type = [FmPopsScore, StudyLocus, TargetIndex]
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
            feature_dependency (dict[str, Any]): Datasets with the fmPops scores, the credible sets and the genes

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
