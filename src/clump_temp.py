"""Clumps GWAS significant variants from summary statistics with a distance based method."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from gentropy.common.utils import split_pvalue
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics
from pyspark.sql import Column, Window

if TYPE_CHECKING:
    from gentropy.dataset.summary_statistics import SummaryStatistics
    from pyspark.sql import Column


class WindowBasedClumping:
    """Get semi-lead snps from summary statistics using a window based function."""

    @staticmethod
    def clump(
        summary_statistics: SummaryStatistics,
        distance: int = 500_000,
        gwas_significance: float = 5e-8,
        collect_locus: bool = False,
        collect_locus_distance: int = 500_000,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics by distance based clumping + collect locus.

        The clumping procedure takes all SNPs that are significant at threshold `gwas_significance` that have not already been clumped by more significant index SNPs. If `collect_locus` is `True`, the method forms clumps of all other SNPs that are within a `collect_locus_distance` from the index SNP (default 500kb). SNPs can appear in more than one locus if independently clumped by 2 non-onverlapping index SNPs.

        Args:
            summary_statistics (SummaryStatistics): Summary statistics to be used for clumping.
            distance (int): Distance in base pairs to be used for clumping. Defaults to 500_000.
            gwas_significance (float, optional): GWAS significance threshold. Defaults to 5e-8.
            collect_locus (bool): Whether to collect locus around semi-indices. Defaults to False.
            collect_locus_distance (int): The distance to collect locus around semi-indices. If not provided, locus is not collected. Defaults to 500_000.

        Returns:
            StudyLocus: Clumped study-locus containing variants based on window.
        """
        # When collect_locus is False, we only want to keep the significant variants
        if not collect_locus:
            summary_statistics = summary_statistics.pvalue_filter(gwas_significance)

        def _pvalue_filter_expr(
            pvalue_threshold: float,
            pValueExponentCol: Column,
            pValueMantissaCol: Column,
        ) -> Column:
            (mantissa, exponent) = split_pvalue(pvalue_threshold)
            return (pValueExponentCol < exponent) | (
                (pValueExponentCol == exponent) & (pValueMantissaCol <= mantissa)
            )

        # STAGE 1:
        # Finding semi-indices (potential lead variants) by evaluating a window around each variant
        # Variants without a significant signal in the window are discarded
        # WARNING: This stage can be time consuming in large chromosomes with complete statistics and many significant associations
        w1 = (
            Window.partitionBy("studyId", "chromosome")
            .orderBy("position")
            .rangeBetween(-distance, distance)
        )
        ss_with_indexes = (
            summary_statistics.df.withColumn(
                "semiIndexId",
                # potential lead variants defined as lowest p-value in window (below threshold)
                f.element_at(
                    f.array_sort(
                        f.collect_list(
                            f.when(
                                _pvalue_filter_expr(
                                    gwas_significance,
                                    f.col("pValueExponent"),
                                    f.col("pValueMantissa"),
                                ),
                                f.struct(
                                    f.col("pValueExponent"),
                                    f.col("pValueMantissa"),
                                    f.col("variantId"),
                                ),
                            )
                        ).over(w1),
                    ),
                    1,
                )["variantId"],
            )
            # discard variants with no semi-index (significant signal in +/- window)
            .filter(f.col("semiIndexId").isNotNull())
        )

        # STAGE 2:
        # If collect_locus is True, we collect all variants in a window around the semi-indices
        # Rows without a locus are discarded as they are already contained in the respective loci
        if collect_locus:
            w2 = (
                Window.partitionBy("studyId", "chromosome")
                .orderBy("position")
                .rangeBetween(-collect_locus_distance, collect_locus_distance)
            )
            ss_with_indexes = (
                ss_with_indexes.withColumn(  # collect variants in locus only for potential leads
                    "locus",
                    f.when(
                        _pvalue_filter_expr(
                            gwas_significance,
                            f.col("pValueExponent"),
                            f.col("pValueMantissa"),
                        ),
                        # f.col("pValueExponent") <= f.lit(-8),
                        f.collect_list(
                            f.struct(
                                f.col("variantId"),
                                f.col("pValueMantissa"),
                                f.col("pValueExponent"),
                                f.col("beta"),
                                f.col("standardError"),
                            )
                        ).over(w2),
                    ),
                )
                # Focus on potential leads (locus contains remaining variants)
                .filter(f.col("locus").isNotNull())
            )

        # STAGE 3:
        # When multiple nominated loci share the same semiIndexId only the most significant remains
        w3 = Window.partitionBy("studyId", "chromosome", "semiIndexId").orderBy(
            f.asc("pValueExponent"), f.asc("pValueMantissa")
        )
        ss_with_indexes_nr = (
            ss_with_indexes.withColumn("row", f.row_number().over(w3))
            .filter(f.col("row") == 1)
            .drop("row", "semiIndexId")
        )
        # Format and return
        return StudyLocus(
            _df=ss_with_indexes_nr.withColumn(
                "studyLocusId",
                StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
            ),
            _schema=StudyLocus.get_schema(),
        )
