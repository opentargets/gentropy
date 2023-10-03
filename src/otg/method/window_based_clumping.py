"""Clumps GWAS significant variants from summary statistics with a distance based method."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from pyspark.ml import functions as fml
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql.window import Window

from otg.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from numpy import ndarray
    from pyspark.sql import Column

    from otg.dataset.summary_statistics import SummaryStatistics


class WindowBasedClumping:
    """Get semi-lead snps from summary statistics using a window based function."""

    # Excluded regions:
    EXCLUDED_REGIONS = ["6:28,510,120-33,480,577"]

    @staticmethod
    def _cluster_peaks(
        study: Column, chromosome: Column, position: Column, window_length: int
    ) -> Column:
        """Cluster GWAS significant variants, were clusters are separated by a defined distance.

        !! Important to note that the length of the clusters can be arbitrarily big.

        Args:
            study (Column): study identifier
            chromosome (Column): chromosome identifier
            position (Column): position of the variant
            window_length (int): window length in basepair

        Returns:
            Column: containing cluster identifier

        Examples:
            >>> data = [
            ...     # Cluster 1:
            ...     ('s1', 'chr1', 2),
            ...     ('s1', 'chr1', 4),
            ...     ('s1', 'chr1', 12),
            ...     # Cluster 2 - Same chromosome:
            ...     ('s1', 'chr1', 31),
            ...     ('s1', 'chr1', 38),
            ...     ('s1', 'chr1', 42),
            ...     # Cluster 3 - New chromosome:
            ...     ('s1', 'chr2', 41),
            ...     ('s1', 'chr2', 44),
            ...     ('s1', 'chr2', 50),
            ...     # Cluster 4 - other study:
            ...     ('s2', 'chr2', 55),
            ...     ('s2', 'chr2', 62),
            ...     ('s2', 'chr2', 70),
            ... ]
            >>> window_length = 10
            >>> (
            ...     spark.createDataFrame(data, ['studyId', 'chromosome', 'position'])
            ...     .withColumn("cluster_id",
            ...         WindowBasedClumping._cluster_peaks(
            ...             f.col('studyId'),
            ...             f.col('chromosome'),
            ...             f.col('position'),
            ...             window_length
            ...         )
            ...     ).show()
            ... )
            +-------+----------+--------+----------+
            |studyId|chromosome|position|cluster_id|
            +-------+----------+--------+----------+
            |     s1|      chr1|       2| s1_chr1_2|
            |     s1|      chr1|       4| s1_chr1_2|
            |     s1|      chr1|      12| s1_chr1_2|
            |     s1|      chr1|      31|s1_chr1_31|
            |     s1|      chr1|      38|s1_chr1_31|
            |     s1|      chr1|      42|s1_chr1_31|
            |     s1|      chr2|      41|s1_chr2_41|
            |     s1|      chr2|      44|s1_chr2_41|
            |     s1|      chr2|      50|s1_chr2_41|
            |     s2|      chr2|      55|s2_chr2_55|
            |     s2|      chr2|      62|s2_chr2_55|
            |     s2|      chr2|      70|s2_chr2_55|
            +-------+----------+--------+----------+
            <BLANKLINE>

        """
        # By adding previous position, the cluster boundary can be identified:
        previous_position = f.lag(position).over(
            Window.partitionBy(study, chromosome).orderBy(position)
        )
        # We consider a cluster boudary if subsequent snps are further than the defined window:
        cluster_id = f.when(
            (previous_position.isNull())
            | (position - previous_position > window_length),
            f.concat_ws("_", study, chromosome, position),
        )
        # The cluster identifier is propagated across every variant of the cluster:
        return f.when(
            cluster_id.isNull(),
            f.last(cluster_id, ignorenulls=True).over(
                Window.partitionBy(study, chromosome)
                .orderBy(position)
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        ).otherwise(cluster_id)

    @staticmethod
    def _prune_peak(position: ndarray, window_size: int) -> DenseVector:
        """Establish lead snps based on their positions listed by p-value.

        The function `find_peak` assigns lead SNPs based on their positions listed by p-value within a specified window size.

        Args:
            position (ndarray): positions of the SNPs sorted by p-value.
            window_size (int): the distance in bp within which associations are clumped together around the lead snp.

        Returns:
            DenseVector: binary vector where 1 indicates a lead SNP and 0 indicates a non-lead SNP.

        Examples:
            >>> from pyspark.ml import functions as fml
            >>> from pyspark.ml.linalg import DenseVector
            >>> WindowBasedClumping._prune_peak(np.array((3, 9, 8, 4, 6)), 2)
            DenseVector([1.0, 1.0, 0.0, 0.0, 1.0])

        """
        # Initializing the lead list with zeroes:
        is_lead: ndarray = np.zeros(len(position))

        # List containing indices of leads:
        lead_indices: list = []

        # Looping through all positions:
        for index in range(len(position)):
            # Looping through leads to find out if they are within a window:
            for lead_index in lead_indices:
                # If any of the leads within the window:
                if abs(position[lead_index] - position[index]) < window_size:
                    # Skipping further checks:
                    break
            else:
                # None of the leads were within the window:
                lead_indices.append(index)
                is_lead[index] = 1

        return DenseVector(is_lead)

    @classmethod
    def clump(
        cls: type[WindowBasedClumping],
        summary_stats: SummaryStatistics,
        window_length: int,
        p_value_significance: float = 5e-8,
    ) -> StudyLocus:
        """Clump summary statistics by distance.

        Args:
            summary_stats (SummaryStatistics): summary statistics to clump
            window_length (int): window length in basepair
            p_value_significance (float): only more significant variants are considered

        Returns:
            StudyLocus: clumped summary statistics
        """
        # Create window for locus clusters
        # - variants where the distance between subsequent variants is below the defined threshold.
        # - Variants are sorted by descending significance
        cluster_window = Window.partitionBy(
            "studyId", "chromosome", "cluster_id"
        ).orderBy(f.col("pValueExponent").asc(), f.col("pValueMantissa").asc())

        return StudyLocus(
            _df=(
                summary_stats
                # Dropping snps below significance - all subsequent steps are done on significant variants:
                .pvalue_filter(p_value_significance)
                .df
                # Clustering summary variants for efficient windowing (complexity reduction):
                .withColumn(
                    "cluster_id",
                    WindowBasedClumping._cluster_peaks(
                        f.col("studyId"),
                        f.col("chromosome"),
                        f.col("position"),
                        window_length,
                    ),
                )
                # Within each cluster variants are ranked by significance:
                .withColumn("pvRank", f.row_number().over(cluster_window))
                # Collect positions in cluster for the most significant variant (complexity reduction):
                .withColumn(
                    "collectedPositions",
                    f.when(
                        f.col("pvRank") == 1,
                        f.collect_list(f.col("position")).over(
                            cluster_window.rowsBetween(
                                Window.currentRow, Window.unboundedFollowing
                            )
                        ),
                    ).otherwise(f.array()),
                )
                # Get semi indices only ONCE per cluster:
                .withColumn(
                    "semiIndices",
                    f.when(
                        f.size(f.col("collectedPositions")) > 0,
                        fml.vector_to_array(
                            f.udf(WindowBasedClumping._prune_peak, VectorUDT())(
                                fml.array_to_vector(f.col("collectedPositions")),
                                f.lit(window_length),
                            )
                        ),
                    ),
                )
                # Propagating the result of the above calculation for all rows:
                .withColumn(
                    "semiIndices",
                    f.when(
                        f.col("semiIndices").isNull(),
                        f.first(f.col("semiIndices"), ignorenulls=True).over(
                            cluster_window
                        ),
                    ).otherwise(f.col("semiIndices")),
                )
                # Keeping semi indices only:
                .filter(f.col("semiIndices")[f.col("pvRank") - 1] > 0)
                .drop("pvRank", "collectedPositions", "semiIndices", "cluster_id")
                # Adding study-locus id:
                .withColumn(
                    "studyLocusId",
                    StudyLocus.assign_study_locus_id("studyId", "variantId"),
                )
            ),
            _schema=StudyLocus.get_schema(),
        )

    @classmethod
    def clump_with_locus(
        cls: type[WindowBasedClumping],
        summary_stats: SummaryStatistics,
        window_length: int,
        p_value_significance: float = 5e-8,
        p_value_baseline: float = 0.05,
        locus_window_length: int | None = None,
    ) -> StudyLocus:
        """Clump significant associations while collecting locus around them.

        Args:
            summary_stats (SummaryStatistics): Input summary statistics dataset
            window_length (int): Window size in  bp, used for distance based clumping and collecting locus
            p_value_significance (float, optional): GWAS significance threshold used to filter peaks. Defaults to 5e-8.
            p_value_baseline (float, optional): Least significant threshold. Below this, all snps are dropped. Defaults to 0.05.
            locus_window_length (int, optional): The distance for collecting locus around the semi indices.

        Returns:
            StudyLocus: StudyLocus after clumping with information about the `locus`
        """
        # If no locus window provided, using the same value:
        if locus_window_length is None:
            locus_window_length = window_length

        # Exclude problematic regions from clumping:
        filtered_summary_stats = reduce(
            lambda df, region: df.exclude_region(region),
            cls.EXCLUDED_REGIONS,
            summary_stats,
        )

        # Run distance based clumping on the summary stats:
        clumped_dataframe = WindowBasedClumping.clump(
            filtered_summary_stats,
            window_length=window_length,
            p_value_significance=p_value_significance,
        ).df.alias("clumped")

        # Extract column names:
        columns = filtered_summary_stats.df.columns

        # Dropping variants not meeting the baseline criteria:
        sumstats_baseline = filtered_summary_stats.pvalue_filter(p_value_baseline).df

        # Renaming columns:
        sumstats_baseline_renamed = sumstats_baseline.selectExpr(
            *[f"{col} as tag_{col}" for col in sumstats_baseline.columns]
        ).alias("sumstat")

        study_locus_df = (
            sumstats_baseline_renamed
            # Joining the two datasets together:
            .join(
                f.broadcast(clumped_dataframe),
                on=[
                    (f.col("sumstat.tag_studyId") == f.col("clumped.studyId"))
                    & (f.col("sumstat.tag_chromosome") == f.col("clumped.chromosome"))
                    & (
                        f.col("sumstat.tag_position")
                        >= (f.col("clumped.position") - locus_window_length)
                    )
                    & (
                        f.col("sumstat.tag_position")
                        <= (f.col("clumped.position") + locus_window_length)
                    )
                ],
                how="right",
            )
            # Filter empty sumstats rows:
            .filter(f.col("tag_studyId").isNotNull())
            .withColumn(
                "locus",
                f.struct(
                    f.col("tag_variantId").alias("variantId"),
                    f.col("tag_beta").alias("beta"),
                    f.col("tag_pValueMantissa").alias("pValueMantissa"),
                    f.col("tag_pValueExponent").alias("pValueExponent"),
                    f.col("tag_standardError").alias("standardError"),
                ),
            )
            .groupby(*columns, "studyLocusId")
            .agg(f.collect_list(f.col("locus")).alias("locus"))
        )
        study_locus_df.show(1, False, True)

        return StudyLocus(
            _df=study_locus_df,
            _schema=StudyLocus.get_schema(),
        )
