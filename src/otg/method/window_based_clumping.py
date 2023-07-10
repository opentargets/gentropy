"""Clumps GWAS significant variants from summary statistics with a distance based method."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from pyspark.ml import functions as fml
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql.window import Window

from otg.common.spark_helpers import calculate_neglog_pvalue
from otg.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from numpy import ndarray
    from pyspark.sql import Column

    from otg.dataset.summary_statistics import SummaryStatistics


class WindowBasedClumping:
    """Get semi-lead snps from summary statistics using a window based function."""

    @staticmethod
    def _identify_cluster_peaks(
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
            ...         WindowBasedClumping._identify_cluster_peaks(
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
    @f.udf(VectorUDT())
    def _find_peak(position: ndarray, window_size: int) -> DenseVector:
        """Establish lead snps based on their positions listed by p-value.

        The function `find_peak` assigns lead SNPs based on their positions listed by p-value within a specified window size.

        Args:
            position (ndarray): positions of the SNPs sorted by p-value.
            window_size (int): the distance in bp within which associations are clumped together around the lead snp.

        Returns:
            DenseVector: binary vector where 1 indicates a lead SNP and 0 indicates a non-lead SNP.

        Examples:
            >>> from pyspark.ml import functions as fml
            >>> data = [
            ...     ('c', 3, 4.0, True),
            ...     ('c', 4, 2.0, False),
            ...     ('c', 6, 1.0, True),
            ...     ('c', 8, 2.5, False),
            ...     ('c', 9, 3.0, True)
            ... ]
            >>> (
            ...     spark.createDataFrame(data, ['cluster', 'position', 'negLogPValue', 'isSemiIndex'])
            ...     .withColumn(
            ...        'collected_positions',
            ...         f.collect_list(
            ...             f.col('position'))
            ...         .over(
            ...             Window.partitionBy('cluster')
            ...             .orderBy(f.col('negLogPValue').desc())
            ...             .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ...         )
            ...     )
            ...     .withColumn('isLeadList', WindowBasedClumping._find_peak(fml.array_to_vector(f.col('collected_positions')), f.lit(2)))
            ...     .show(truncate=False)
            ... )
            +-------+--------+------------+-----------+-------------------+---------------------+
            |cluster|position|negLogPValue|isSemiIndex|collected_positions|isLeadList           |
            +-------+--------+------------+-----------+-------------------+---------------------+
            |c      |3       |4.0         |true       |[3, 9, 8, 4, 6]    |[1.0,1.0,0.0,0.0,1.0]|
            |c      |9       |3.0         |true       |[3, 9, 8, 4, 6]    |[1.0,1.0,0.0,0.0,1.0]|
            |c      |8       |2.5         |false      |[3, 9, 8, 4, 6]    |[1.0,1.0,0.0,0.0,1.0]|
            |c      |4       |2.0         |false      |[3, 9, 8, 4, 6]    |[1.0,1.0,0.0,0.0,1.0]|
            |c      |6       |1.0         |true       |[3, 9, 8, 4, 6]    |[1.0,1.0,0.0,0.0,1.0]|
            +-------+--------+------------+-----------+-------------------+---------------------+
            <BLANKLINE>

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

    @staticmethod
    def _filter_leads(clump: Column, window_length: int) -> Column:
        """Filter lead snps from a column containing clumps with prioritised variants.

        Args:
            clump (Column): column containing array of structs with all variants in the clump sorted by priority.
            window_length (int): window length in basepair

        Returns:
            Column: column containing array of structs with only lead variants.

        Examples:
            >>> data = [
            ...     ('v6', 10),
            ...     ('v4', 6),
            ...     ('v1', 3),
            ...     ('v2', 4),
            ...     ('v3', 5),
            ...     ('v5', 8),
            ...     ('v7', 13),
            ...     ('v8', 20)
            ... ]
            >>> window_length = 2
            >>> (
            ...    spark.createDataFrame(data, ['variantId', 'position']).withColumn("study", f.lit("s1"))
            ...    .groupBy("study")
            ...    .agg(f.collect_list(f.struct("*")).alias("clump"))
            ...    .select(WindowBasedClumping._filter_leads(f.col('clump'), window_length).alias("filtered_clump"))
            ...    .show(truncate=False)
            ... )
            +---------------------------------------------------------------------------------------------------------------+
            |filtered_clump                                                                                                 |
            +---------------------------------------------------------------------------------------------------------------+
            |[{v6, 10, s1, 1.0}, {v4, 6, s1, 1.0}, {v1, 3, s1, 1.0}, {v5, 8, s1, 1.0}, {v7, 13, s1, 1.0}, {v8, 20, s1, 1.0}]|
            +---------------------------------------------------------------------------------------------------------------+
            <BLANKLINE>

        """
        # Combine the lead position vector with the aggregated fields and dropping non-lead snps:
        return f.filter(
            f.zip_with(
                clump,
                # Extract the position vector and identify positions of the leads:
                fml.vector_to_array(
                    WindowBasedClumping._find_peak(
                        fml.array_to_vector(f.transform(clump, lambda x: x.position)),
                        f.lit(window_length),
                    )
                ),
                lambda x, y: f.when(y == 1.0, x.withField("isLead", y)),
            ),
            lambda col: col.isNotNull(),
        )

    @staticmethod
    def _collect_clump(mantissa: Column, exponent: Column) -> Column:
        """Collect clump into a sorted struct.

        Args:
            mantissa (Column): mantissa of the p-value
            exponent (Column): exponent of the p-value

        Returns:
            Column: struct containing clumped variants sorted by negLogPValue in descending order

        Examples:
            >>> data = [
            ...     ('s1', 'chr1', 2, 0.1, -1),
            ...     ('s1', 'chr1', 4, 0.2, -1),
            ...     ('s1', 'chr1', 12, 0.3, -1),
            ...     ('s1', 'chr1', 31, 0.4, -1),
            ...     ('s1', 'chr1', 38, 0.5, -1),
            ...     ('s1', 'chr1', 42, 0.6, -1),
            ...     ('s1', 'chr2', 41, 0.7, -1),
            ...     ('s1', 'chr2', 44, 0.8, -1),
            ...     ('s1', 'chr2', 50, 0.9, -1),
            ...     ('s2', 'chr2', 55, 1.0, -1),
            ...     ('s2', 'chr2', 62, 1.1, -1),
            ...     ('s2', 'chr2', 70, 1.2, -1),
            ... ]
            >>> (
            ...    spark.createDataFrame(data, ['studyId', 'chromosome', 'position', 'pValueMantissa', 'pValueExponent'])
            ...     .groupBy('studyId', 'chromosome')
            ...     .agg(WindowBasedClumping._collect_clump(
            ...                 f.col('pValueMantissa'),
            ...                 f.col('pValueExponent')
            ...             ).alias("clump")
            ...     ).show(truncate=False)
            ... )
            +-------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            |studyId|chromosome|clump                                                                                                                                                                                                                                                        |
            +-------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            |s1     |chr1      |[{2.0, s1, chr1, 2, 0.1, -1}, {1.6989700043360187, s1, chr1, 4, 0.2, -1}, {1.5228787452803376, s1, chr1, 12, 0.3, -1}, {1.3979400086720375, s1, chr1, 31, 0.4, -1}, {1.3010299956639813, s1, chr1, 38, 0.5, -1}, {1.2218487496163564, s1, chr1, 42, 0.6, -1}]|
            |s1     |chr2      |[{1.154901959985743, s1, chr2, 41, 0.7, -1}, {1.0969100130080565, s1, chr2, 44, 0.8, -1}, {1.045757490560675, s1, chr2, 50, 0.9, -1}]                                                                                                                        |
            |s2     |chr2      |[{1.0, s2, chr2, 55, 1.0, -1}, {0.958607314841775, s2, chr2, 62, 1.1, -1}, {0.9208187539523752, s2, chr2, 70, 1.2, -1}]                                                                                                                                      |
            +-------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            <BLANKLINE>

        """
        return f.sort_array(
            f.collect_list(
                f.struct(
                    calculate_neglog_pvalue(mantissa, exponent).alias("negLogPValue"),
                    "*",
                )
            ),
            False,
        )

    @classmethod
    def clump(
        cls: type[WindowBasedClumping],
        summary_stats: SummaryStatistics,
        window_length: int,
    ) -> StudyLocus:
        """Clump summary statistics by distance.

        Args:
            summary_stats (SummaryStatistics): summary statistics to clump
            window_length (int): window length in basepair

        Returns:
            StudyLocus: clumped summary statistics
        """
        return StudyLocus(
            _df=summary_stats.df.withColumn(
                "cluster_id",
               # First identify clusters of variants within the window
                WindowBasedClumping._identify_cluster_peaks(
                    f.col("studyId"),
                    f.col("chromosome"),
                    f.col("position"),
                    window_length,
                ),
            )
            .groupBy("cluster_id")
            # Aggregating all data from each cluster:
            .agg(
                WindowBasedClumping._collect_clump(
                    f.col("pValueMantissa"), f.col("pValueExponent")
                ).alias("clump")
            )
            # Explode and extract columns:
            .withColumn(
                "exploded",
                f.explode(
                    WindowBasedClumping._filter_leads(f.col("clump"), window_length)
                ),
            )
            .select("exploded.*")
            # Dropping helper columns:
            .drop("isLead", "negLogPValue")
        )
