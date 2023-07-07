"""Clumps GWAS significant variants from summary statistics with a distance based method."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from pyspark.ml import functions as fml
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql.window import Window

from otg.common.spark_helpers import calculate_neglog_pvalue

if TYPE_CHECKING:
    # from otg.dataset.study_locus import StudyLocus
    from numpy import ndarray
    from pyspark.sql import DataFrame


class WindowBasedClumping:
    """Get semi-lead snps from summary statistics using a window based function."""

    @staticmethod
    def cluster_peaks(df: DataFrame, window_length: int) -> DataFrame:
        """Cluster GWAS significant variants, were clusters are separated by a defined distance.

        !! Important to note that the length of the clusters can be arbitrarily big.

        Args:
            df (DataFrame): table with studyId, chromosome and position columns
            window_length (int): a minimal basepair distance required between snps to call for a new cluster

        Returns:
            DataFrame: with cluster_id column added.

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
            ...     .transform(lambda df: WindowBasedClumping.cluster_peaks(df, window_length))
            ...     .show()
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
        return (
            df
            # By adding previous position, the cluster boundary can be identified:
            .withColumn(
                "previous_position",
                f.lag("position").over(
                    Window.partitionBy("studyId", "chromosome").orderBy("position")
                ),
            )
            # We consider a cluster boudary if subsequent snps are further than the defined window:
            .withColumn(
                "cluster_id",
                f.when(
                    (f.col("previous_position").isNull())
                    | (f.col("position") - f.col("previous_position") > window_length),
                    f.concat_ws(
                        "_", f.col("studyId"), f.col("chromosome"), f.col("position")
                    ),
                ),
            )
            # The cluster identifier is propagated across every variant of the cluster:
            .withColumn(
                "cluster_id",
                f.when(
                    f.col("cluster_id").isNull(),
                    f.last("cluster_id", ignorenulls=True).over(
                        Window.partitionBy("studyId", "chromosome")
                        .orderBy("position")
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                    ),
                ).otherwise(f.col("cluster_id")),
            ).drop("previous_position")
        )

    @staticmethod
    @f.udf(VectorUDT())
    def find_peak(position: ndarray, window_size: int) -> DenseVector:
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
            ...     .withColumn('isLeadList', WindowBasedClumping.find_peak(fml.array_to_vector(f.col('collected_positions')), f.lit(2)))
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

    @classmethod
    def find_index_snps(
        cls: type[WindowBasedClumping], df: DataFrame, window_length: int
    ) -> DataFrame:
        """Finding index snps based on window.

        Args:
            df (DataFrame): dataframe extracted from p-value filtered summary statistics
            window_length (int): window lenght in basepair

        Returns:
            DataFrame: Dataframe containing only lead snps
        """
        return (
            df.transform(lambda df: cls.cluster_peaks(df, window_length))
            .groupBy("cluster_id")
            # Aggregating all data from each cluster:
            .agg(
                f.sort_array(
                    f.collect_list(
                        f.struct(
                            calculate_neglog_pvalue(
                                f.col("pValueMantissa"), f.col("pValueExponent")
                            ).alias("negLogPValue"),
                            "*",
                        )
                    ),
                    False,
                ).alias("aggregatedCluster")
            )
            # Extract the position vector and identify positions of the leads:
            .withColumn(
                "isLeadList",
                fml.vector_to_array(
                    cls.find_peak(
                        fml.array_to_vector(
                            f.transform(
                                f.col("aggregatedCluster"), lambda x: x.position
                            )
                        ),
                        f.lit(window_length),
                    )
                ),
            )
            # Combine the lead position vector with the aggregated fields and dropping non-lead snps:
            .withColumn(
                "combinedList",
                f.filter(
                    f.zip_with(
                        f.col("aggregatedCluster"),
                        f.col("isLeadList"),
                        lambda x, y: f.when(y == 1.0, x.withField("isLead", y)),
                    ),
                    lambda col: col.isNotNull(),
                ),
            )
            # Explode and extract columns:
            .withColumn("exploded", f.explode(f.col("combinedList")))
            .select("exploded.*")
            # Dropping helper columns:
            .drop("isLead", "negLogPValue")
        )
