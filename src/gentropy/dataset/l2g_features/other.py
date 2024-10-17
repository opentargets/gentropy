"""Methods to generate features which are not obviously categorised."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f

from gentropy.common.spark_helpers import convert_from_wide_to_long
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def common_genecount_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    gene_index: GeneIndex,
    feature_name: str,
    genomic_window: int,
    protein_coding_only: bool = False,
) -> DataFrame:
    """Computes the feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        gene_index (GeneIndex): Dataset containing information related to all genes in release.
        feature_name (str): The name of the feature
        genomic_window (int): The maximum window size to consider
        protein_coding_only (bool): Whether to only consider protein coding genes in calculation.

    Returns:
            DataFrame: Feature dataset
    """
    study_loci_window = (
        study_loci_to_annotate.df.withColumn(
            "window_start", f.col("position") - (genomic_window / 2)
        )
        .withColumn("window_end", f.col("position") + (genomic_window / 2))
        .withColumnRenamed("chromosome", "SL_chromosome")
    )

    if protein_coding_only:
        gene_index.df = gene_index.df.filter(f.col("biotype") == "protein_coding")

    distinct_gene_counts = (
        study_loci_window.join(
            gene_index.df.alias("genes"),
            on=(
                (f.col("SL_chromosome") == f.col("genes.chromosome"))
                & (f.col("genes.tss") >= f.col("window_start"))
                & (f.col("genes.tss") <= f.col("window_end"))
            ),
            how="inner",
        )
        .groupBy("studyLocusId")
        .agg(f.countDistinct("geneId").alias(feature_name))
    )

    return (
        study_loci_window.join(
            gene_index.df.alias("genes"),
            on=(
                (f.col("SL_chromosome") == f.col("genes.chromosome"))
                & (f.col("genes.tss") >= f.col("window_start"))
                & (f.col("genes.tss") <= f.col("window_end"))
            ),
            how="inner",
        )
        .join(distinct_gene_counts, on="studyLocusId", how="inner")
        .select("studyLocusId", "geneId", feature_name)
        .distinct()
    )


class GeneCountFeature(L2GFeature):
    """Counts the number of genes within a specified window size from the study locus."""

    fill_na_value = 0  # Default fill value if gene count is missing
    feature_dependency_type = GeneIndex
    feature_name = "geneCount500kb"

    @classmethod
    def compute(
        cls: type[GeneCountFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> GeneCountFeature:
        """Computes the gene count feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dictionary containing dependencies, with gene index and window size

        Returns:
            GeneCountFeature: Feature dataset
        """
        genomic_window = 500000
        gene_count_df = common_genecount_feature_logic(
            study_loci_to_annotate=study_loci_to_annotate,
            feature_name=cls.feature_name,
            genomic_window=genomic_window,
            **feature_dependency,
        )

        return cls(
            _df=convert_from_wide_to_long(
                gene_count_df,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class ProteinGeneCountFeature(L2GFeature):
    """Counts the number of protein coding genes within a specified window size from the study locus."""

    fill_na_value = 0  # Default fill value if gene count is missing
    feature_dependency_type = GeneIndex
    feature_name = "proteinGeneCount500kb"

    @classmethod
    def compute(
        cls: type[ProteinGeneCountFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> ProteinGeneCountFeature:
        """Computes the gene count feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dictionary containing dependencies, with gene index and window size

        Returns:
            GeneCountFeature: Feature dataset
        """
        genomic_window = 500000
        gene_count_df = common_genecount_feature_logic(
            study_loci_to_annotate=study_loci_to_annotate,
            feature_name=cls.feature_name,
            genomic_window=genomic_window,
            protein_coding_only=True,
            **feature_dependency,
        )

        return cls(
            _df=convert_from_wide_to_long(
                gene_count_df,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
