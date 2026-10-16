"""Locus overlap dataset: pairs of loci that share a tag variant, with their aligned statistics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import types as t

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import AnnotatedStudyLocus

# Statistics of each tag, aligned as `left_<name>` and `right_<name>`.
_STATISTICS = [
    "logBF",
    "posteriorProbability",
    "beta",
    "pValueMantissa",
    "pValueExponent",
]


@dataclass
class LocusOverlap(Dataset):
    """Locus overlap.

    Pairs of loci, of credible sets or of study loci, that share at least one tag variant.
    `leftLocusId` and `rightLocusId` are credible set ids or study locus ids, according to the
    builder. The left locus is always a GWAS. By default all GWAS-GWAS and all GWAS-molecular
    trait pairs are found, with the molecular trait always on the right.

    Each row is one tag variant of a pair, from either or both loci, with the statistics of
    each side:

    - from credible sets (`from_credible_sets`): the natural log Bayes factor (`logBF`), the
      posterior probability, the marginal (`reportedBeta`) beta and the p-value;
    - from study loci (`from_study_loci`): the marginal beta and the p-value, with a null Bayes
      factor and posterior probability.

    !!! note

        This is a helpful dataset for other downstream analyses, such as colocalisation.
    """

    @classmethod
    def get_schema(cls: type[LocusOverlap]) -> StructType:
        """Provides the schema for the LocusOverlap dataset.

        Returns:
            StructType: Schema for the LocusOverlap dataset
        """
        return parse_spark_schema("locus_overlap.json")

    @classmethod
    def from_credible_sets(
        cls: type[LocusOverlap],
        credible_sets: CredibleSet,
        credible_set_variants: CredibleSetVariant,
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
    ) -> LocusOverlap:
        """Find the credible sets sharing a variant.

        Args:
            credible_sets (CredibleSet): Credible sets.
            credible_set_variants (CredibleSetVariant): Their variants, the tags of the overlaps.
            restrict_right_studies (list[str] | None): List of studyIds to restrict finding overlaps to on the right-side. Default is None.
            gwas_v_qtl_overlap_only (bool): If True, finds overlaps between all GWAS loci vs molecular-QTLs. Default is False.

        Returns:
            LocusOverlap: Pairs of overlapping credible sets with aligned tags.

        Examples:
            >>> from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
            >>> def credible_set(id, study, study_type):
            ...     return (id, "1", 1, 100, study, study_type, "1_10_A_T", "SuSiE", None, None,
            ...             None, None, [])
            >>> def variant(id, variant, log_bf):
            ...     return (id, variant, 1.0, -8, None, None, 0.1, 0.01, 0.5, log_bf, None,
            ...             None, None, None, None, None, [])
            >>> credible_sets = CredibleSet(spark.createDataFrame(
            ...     [credible_set("gwas", "S1", "gwas"), credible_set("eqtl", "S2", "eqtl")],
            ...     CredibleSet.get_schema(),
            ... ))
            >>> variants = CredibleSetVariant(spark.createDataFrame(
            ...     [variant("gwas", "1_10_A_T", 2.0), variant("gwas", "1_20_C_G", 1.0),
            ...      variant("eqtl", "1_10_A_T", 3.0)],
            ...     CredibleSetVariant.get_schema(),
            ... ))
            >>> overlaps = LocusOverlap.from_credible_sets(credible_sets, variants)
            >>> overlaps.df.select(
            ...     "leftLocusId", "rightLocusId", "tagVariantId",
            ...     "statistics.left_logBF", "statistics.right_logBF",
            ... ).orderBy("tagVariantId").show()
            +-----------+------------+------------+----------+-----------+
            |leftLocusId|rightLocusId|tagVariantId|left_logBF|right_logBF|
            +-----------+------------+------------+----------+-----------+
            |       gwas|        eqtl|    1_10_A_T|       2.0|        3.0|
            |       gwas|        eqtl|    1_20_C_G|       1.0|       NULL|
            +-----------+------------+------------+----------+-----------+
            <BLANKLINE>
        """
        tags = credible_set_variants.df.join(
            credible_sets.df.select(
                f.col("id").alias("credibleSetId"),
                "studyId",
                "studyType",
                "chromosome",
            ),
            on="credibleSetId",
        ).select(
            f.col("credibleSetId").alias("locusId"),
            "studyId",
            "studyType",
            "chromosome",
            f.col("variantId").alias("tagVariantId"),
            f.col("logBayesFactor").cast(t.DoubleType()).alias("logBF"),
            f.col("posteriorProbability").cast(t.DoubleType()),
            f.col("reportedBeta").alias("beta"),
            "pValueMantissa",
            "pValueExponent",
        )
        return cls._from_tags(tags, restrict_right_studies, gwas_v_qtl_overlap_only)

    @classmethod
    def from_study_loci(
        cls: type[LocusOverlap],
        study_loci: AnnotatedStudyLocus,
        study_index: StudyIndex,
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
    ) -> LocusOverlap:
        """Find the study loci sharing a variant of their `locus`.

        Study loci have no study type, so it is taken from the study index; loci of a study not in
        it are left out. Their `locus` has no Bayes factor or posterior probability, so those are
        null.

        Args:
            study_loci (AnnotatedStudyLocus): Study loci, whose `locus` variants are the tags.
            study_index (StudyIndex): The studies' types.
            restrict_right_studies (list[str] | None): List of studyIds to restrict finding overlaps to on the right-side. Default is None.
            gwas_v_qtl_overlap_only (bool): If True, finds overlaps between all GWAS loci vs molecular-QTLs. Default is False.

        Returns:
            LocusOverlap: Pairs of overlapping study loci with aligned tags.
        """
        tags = (
            study_loci.df.join(
                study_index.df.select("studyId", "studyType"), on="studyId"
            )
            .filter(f.col("studyType").isNotNull())
            .withColumn("locus", f.explode("locus"))
            .select(
                f.col("id").alias("locusId"),
                "studyId",
                "studyType",
                "chromosome",
                f.col("locus.variantId").alias("tagVariantId"),
                f.lit(None).cast(t.DoubleType()).alias("logBF"),
                f.lit(None).cast(t.DoubleType()).alias("posteriorProbability"),
                f.col("locus.beta").alias("beta"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
            )
        )
        return cls._from_tags(tags, restrict_right_studies, gwas_v_qtl_overlap_only)

    @classmethod
    def _from_tags(
        cls: type[LocusOverlap],
        tags: DataFrame,
        restrict_right_studies: list[str] | None,
        gwas_v_qtl_overlap_only: bool,
    ) -> LocusOverlap:
        """Find the overlapping loci and align their tags.

        Args:
            tags (DataFrame): One row per tag of a locus, with `locusId`, `studyId`, `studyType`,
                `chromosome`, `tagVariantId` and the `_STATISTICS`.
            restrict_right_studies (list[str] | None): List of studyIds to restrict finding overlaps to on the right-side.
            gwas_v_qtl_overlap_only (bool): If True, finds overlaps between all GWAS loci vs molecular-QTLs.

        Returns:
            LocusOverlap: Pairs of overlapping loci with aligned tags.
        """
        loci_to_overlap = tags.persist()
        peak_overlaps = cls._overlapping_peaks(
            loci_to_overlap,
            restrict_right_studies=restrict_right_studies,
            gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
        )
        return cls._align_overlapping_tags(loci_to_overlap, peak_overlaps)

    @staticmethod
    def _overlapping_peaks(
        credset_to_overlap: DataFrame,
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
    ) -> DataFrame:
        """Calculate overlapping signals (loci) between GWAS-GWAS and GWAS-Molecular trait.

        Args:
            credset_to_overlap (DataFrame): DataFrame containing at least `locusId`, `studyId`, `studyType`, `chromosome` and `tagVariantId` columns.
            restrict_right_studies (list[str] | None): List of studyIds to restrict finding overlaps on the right side. Default is None.
            gwas_v_qtl_overlap_only (bool): When True, finds overlaps with just molecular-QTLs on the right-hand side. Default is False.

        Returns:
            DataFrame: containing `leftLocusId`, `rightLocusId` and `chromosome` columns.
        """
        # Reduce columns to the minimum to reduce the size of the dataframe
        credset_to_overlap = credset_to_overlap.select(
            "locusId",
            "studyId",
            "studyType",
            "chromosome",
            "tagVariantId",
        )
        # Define join condition
        # If gwas_v_qtl_overlap_only is True, finds only gwas vs molQTL overlaps.
        if gwas_v_qtl_overlap_only:
            join_condition = [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                f.col("left.studyType") == f.lit("gwas"),
                f.col("right.studyType") != f.lit("gwas"),
            ]
        # If restrict_right_studies is not empty, restrict finding overlaps to those studies
        elif restrict_right_studies is not None:
            join_condition = [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                f.col("left.studyType") == f.lit("gwas"),
                (f.col("right.studyId").isin(restrict_right_studies))
                & (~f.col("left.studyId").isin(restrict_right_studies))
                & (f.col("left.locusId") != f.col("right.locusId")),
            ]
        else:
            join_condition = [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                (f.col("right.studyType") != "gwas")
                | (f.col("left.locusId") > f.col("right.locusId")),
                f.col("left.studyType") == f.lit("gwas"),
            ]

        return (
            credset_to_overlap.alias("left")
            # Self join with complex condition.
            .join(
                credset_to_overlap.alias("right"),
                on=join_condition,
                how="inner",
            )
            .select(
                f.col("left.locusId").alias("leftLocusId"),
                f.col("right.locusId").alias("rightLocusId"),
                f.col("right.studyType").alias("rightStudyType"),
                f.col("left.chromosome").alias("chromosome"),
            )
            .distinct()
            # NOTE: avoid `.repartition("chromosome")` here: with ~25 distinct
            # chromosomes it collapses the result into a handful of severely
            # skewed partitions (chr6/HLA dominates), capping downstream
            # parallelism and risking spill/OOM. `.distinct()` already produces
            # balanced `spark.sql.shuffle.partitions` partitions.
            .persist()
        )

    @staticmethod
    def _align_overlapping_tags(
        loci_to_overlap: DataFrame, peak_overlaps: DataFrame
    ) -> LocusOverlap:
        """Align overlapping tags in pairs of overlapping loci, keeping all tags in both loci.

        Args:
            loci_to_overlap (DataFrame): containing `locusId`, `chromosome`, `tagVariantId` and the `_STATISTICS` columns.
            peak_overlaps (DataFrame): containing `leftLocusId`, `rightLocusId` and `chromosome` columns.

        Returns:
            LocusOverlap: Pairs of overlapping loci with aligned tags.
        """
        # Complete information about all tags in the left locus of the overlap
        overlapping_left = loci_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("locusId").alias("leftLocusId"),
            *[f.col(col).alias(f"left_{col}") for col in _STATISTICS],
        ).join(peak_overlaps, on=["chromosome", "leftLocusId"], how="inner")

        # Complete information about all tags in the right locus of the overlap
        overlapping_right = loci_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("locusId").alias("rightLocusId"),
            *[f.col(col).alias(f"right_{col}") for col in _STATISTICS],
        ).join(peak_overlaps, on=["chromosome", "rightLocusId"], how="inner")

        # Include information about all tag variants in both loci aligned by tag variant id
        overlaps = overlapping_left.join(
            overlapping_right,
            on=[
                "chromosome",
                "rightLocusId",
                "leftLocusId",
                "tagVariantId",
                "rightStudyType",
            ],
            how="outer",
        ).select(
            "leftLocusId",
            "rightLocusId",
            "rightStudyType",
            "chromosome",
            "tagVariantId",
            f.struct(
                *[f"left_{e}" for e in _STATISTICS]
                + [f"right_{e}" for e in _STATISTICS]
            ).alias("statistics"),
        )
        return LocusOverlap(
            _df=overlaps,
            _schema=LocusOverlap.get_schema(),
        )

    def calculate_beta_ratio(self: LocusOverlap) -> DataFrame:
        """Calculate the beta ratio for the overlapping signals.

        Returns:
            DataFrame: A dataframe containing left and right loci IDs, chromosome
            and the average sign of the beta ratio
        """
        return (
            # Unpack statistics column:
            self.df.select("*", "statistics.*")
            .drop("statistics")
            # Drop any rows where the beta is null or zero
            .filter(
                f.col("left_beta").isNotNull()
                & f.col("right_beta").isNotNull()
                & (f.col("left_beta") != 0)
                & (f.col("right_beta") != 0)
            )
            # Calculate the beta ratio and get the sign, then calculate the average sign across all variants in the locus
            .withColumn(
                "betaRatioSign", f.signum(f.col("left_beta") / f.col("right_beta"))
            )
            # Aggregate beta signs:
            .groupBy("leftLocusId", "rightLocusId", "chromosome")
            .agg(f.avg("betaRatioSign").alias("betaRatioSignAverage"))
        )

    def _convert_to_square_matrix(self: LocusOverlap) -> LocusOverlap:
        """Convert the dataset to a square matrix.

        Returns:
            LocusOverlap: Square matrix of the dataset
        """
        return LocusOverlap(
            _df=self.df.unionByName(
                self.df.selectExpr(
                    "leftLocusId as rightLocusId",
                    "rightLocusId as leftLocusId",
                    "rightStudyType",
                    "tagVariantId",
                    "chromosome",
                    "statistics",
                )
            ).distinct(),
            _schema=self.get_schema(),
        )
