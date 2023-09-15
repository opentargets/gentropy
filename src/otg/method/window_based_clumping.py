"""Clumps GWAS significant variants from summary statistics with a distance based method."""

from __future__ import annotations

from typing import TYPE_CHECKING, List

import numpy as np
import pyspark.sql.functions as f
from pyspark.ml import functions as fml
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql.window import Window

from otg.common.spark_helpers import calculate_neglog_pvalue
from otg.common.utils import split_pvalue
from otg.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from numpy import ndarray
    from pyspark.sql import Column, DataFrame

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
            ...     ('clump_1', 2, 0.1, -1),
            ...     ('clump_1', 4, 0.2, -1),
            ...     ('clump_1', 12, 0.3, -1),
            ...     ('clump_1', 31, 0.4, -1),
            ...     ('clump_1', 38, 0.5, -1),
            ...     ('clump_1', 42, 0.6, -1),
            ...     ('clump_2', 41, 0.7, -1),
            ...     ('clump_2', 44, 0.8, -1),
            ...     ('clump_2', 50, 0.9, -1),
            ...     ('clump_3', 55, 1.0, -1),
            ...     ('clump_3', 62, 1.1, -1),
            ...     ('clump_3', 70, 1.2, -1),
            ... ]
            >>> (
            ...    spark.createDataFrame(data, ['clump_id', 'position', 'pValueMantissa', 'pValueExponent'])
            ...     .groupBy('clump_id')
            ...     .agg(WindowBasedClumping._collect_clump(
            ...                 f.col('pValueMantissa'),
            ...                 f.col('pValueExponent')
            ...             ).alias("clump")
            ...     ).show(truncate=False)
            ... )
            +--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            |clump_id|clump                                                                                                                                                                                                                                                  |
            +--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            |clump_1 |[{2.0, clump_1, 2, 0.1, -1}, {1.6989700043360187, clump_1, 4, 0.2, -1}, {1.5228787452803376, clump_1, 12, 0.3, -1}, {1.3979400086720375, clump_1, 31, 0.4, -1}, {1.3010299956639813, clump_1, 38, 0.5, -1}, {1.2218487496163564, clump_1, 42, 0.6, -1}]|
            |clump_2 |[{1.154901959985743, clump_2, 41, 0.7, -1}, {1.0969100130080565, clump_2, 44, 0.8, -1}, {1.045757490560675, clump_2, 50, 0.9, -1}]                                                                                                                     |
            |clump_3 |[{1.0, clump_3, 55, 1.0, -1}, {0.958607314841775, clump_3, 62, 1.1, -1}, {0.9208187539523752, clump_3, 70, 1.2, -1}]                                                                                                                                   |
            +--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
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

    @staticmethod
    def _cluster_sumstats(
        df: DataFrame, window_size: int, pvalue_threshold: float
    ) -> DataFrame:
        """Clustering summary statistics data based on window size and p-value threshold.

        Single point associations are dropped if they are below the p-value threshold OR their
        distance from a significant variant is above the distance threshold.

        Args:
            df (DataFrame): A DataFrame containing the summary statistics data.
            window_size (int): bp distance required between a sub significant and significant variant
            pvalue_threshold (float): p-value marking significance.

        Returns:
            DataFrame pruned data, with cluster id column.
        """
        (threshold_mantissa, threshold_exponent) = split_pvalue(pvalue_threshold)

        clustering_window = Window.partitionBy("studyId", "chromosome")

        filter_expression = (f.col("pValueExponent") < threshold_exponent) | (
            (f.col("pValueExponent") == threshold_exponent)
            & (f.col("PValueMantissa") <= threshold_mantissa)
        )

        return (
            df
            # Flagging significant snps:
            .withColumn(
                "lead_pos", f.when(filter_expression, f.col("position")).otherwise(None)
            )
            # Propagate significant snp position to the neighbouring snps:
            .withColumn(
                "previous_lead",
                f.when(
                    f.col("lead_pos").isNull(),
                    f.last(f.col("lead_pos"), ignorenulls=True).over(
                        clustering_window.orderBy(f.col("position"))
                    ),
                ).otherwise(f.col("lead_pos")),
            )
            .withColumn(
                "next_lead",
                f.when(
                    f.col("lead_pos").isNull(),
                    f.last(f.col("lead_pos"), ignorenulls=True).over(
                        clustering_window.orderBy(f.col("position").desc())
                    ),
                ).otherwise(f.col("lead_pos")),
            )
            # Filter for snps that are closer to a significant snp than the threshold:
            .filter(
                f.array_min(
                    f.array(
                        (f.col("position") - f.col("previous_lead")),
                        (f.col("next_lead") - f.col("position")),
                    )
                )
                <= window_size
            )
            .withColumn(
                "cluster_start",
                f.when(f.col("previous_lead").isNull(), f.col("position"))
                .when(
                    ((f.col("next_lead") - f.col("previous_lead") > window_size))
                    & ((f.col("position") - f.col("previous_lead")) > window_size),
                    f.col("position"),
                )
                .otherwise(None),
            )
            .withColumn(
                "cluster_end",
                f.when(f.col("next_lead").isNull(), f.col("position"))
                .when(
                    ((f.col("next_lead") - f.col("previous_lead") > window_size))
                    & ((f.col("next_lead") - f.col("position")) > window_size),
                    f.col("position"),
                )
                .otherwise(None),
            )
            # Filling up the gaps:
            .withColumn(
                "cluster_start",
                f.when(
                    f.col("cluster_start").isNull(),
                    f.last(f.col("cluster_start"), ignorenulls=True).over(
                        clustering_window.orderBy(f.col("position").asc())
                    ),
                ).otherwise(f.col("cluster_start")),
            )
            .withColumn(
                "cluster_end",
                f.when(
                    f.col("cluster_end").isNull(),
                    f.last(f.col("cluster_end"), ignorenulls=True).over(
                        clustering_window.orderBy(f.col("position").desc())
                    ),
                ).otherwise(f.col("cluster_end")),
            )
            .withColumn(
                "cluster_id",
                f.concat_ws(
                    "_",
                    f.col("studyId"),
                    f.col("chromosome"),
                    f.col("cluster_start"),
                    f.col("cluster_end"),
                ),
            )
            .drop(
                "lead_pos",
                "previous_lead",
                "next_lead",
                "distance",
                "cluster_start",
                "cluster_end",
            )
        )

    @staticmethod
    def collect_locus(
        study_id: Column,
        chromosome: Column,
        position: Column,
        window_length: int,
        collected_columns: List[Column],
    ) -> Column:
        """Collect a list of specified columns within a specified window position.

        Args:
            study_id (Column): A column representing the study ID.
            chromosome (Column): A column representing the chromosome of a genetic locus.
            position (Column): The `position` parameter represents the position of a locus
            window_length (int): Distance around variant collected into a locus object.
            collected_columns (List[Column]): A list of columns that you want to collect for each locus.

        Returns:
            Column: locus object is a list of statistical parameters of the surrounding single point
            associations within the specified window.
        """
        # Defining the locus window:
        locus_window = (
            Window.partitionBy(study_id, chromosome)
            .orderBy(position)
            .rangeBetween(
                Window.currentRow - window_length,
                Window.currentRow + window_length,
            )
        )

        # Collecting the requested columns within the window:
        return f.collect_list(f.struct(*collected_columns)).over(locus_window)

    @classmethod
    def clump(
        cls: type[WindowBasedClumping],
        summary_stats: SummaryStatistics,
        window_length: int,
        p_value_significance: float = 5e-8,
        p_value_baseline: float = 0.05,
    ) -> StudyLocus:
        """Clump summary statistics by distance.

        Args:
            summary_stats (SummaryStatistics): summary statistics to clump
            window_length (int): window length in basepair
            p_value_baseline (float): above this p-value snps will not be collected into locus
            p_value_significance (float): above this p-value threshol snps will not be considered as index snps

        Returns:
            StudyLocus: clumped summary statistics
        """
        # Create a window with a defined length:
        locus_window = (
            Window.partitionBy("studyId", "chromosome")
            .orderBy("position")
            .rangeBetween(
                Window.currentRow - window_length,
                Window.currentRow + window_length,
            )
        )

        # Create window for locus clusters:
        cluster_window = Window.partitionBy(
            "studyId", "chromosome", "cluster_id"
        ).orderBy(f.col("pValueExponent").asc(), f.col("pValueMantissa").asc())

        # Splitting p-value into mantissa and exponent:
        (mantissa_threshold, exponent_threshold) = split_pvalue(p_value_significance)

        # This filter expression will be used twice: once to collect locus data and when we filter for significant
        filter_expression = (f.col("pValueExponent") < exponent_threshold) | (
            (f.col("pValueExponent") == exponent_threshold)
            & (f.col("pValueMantissa") <= mantissa_threshold)
        )

        return StudyLocus(
            _df=(
                summary_stats
                # Dropping all snps below p-value of interest:
                .pvalue_filter(p_value_baseline)
                .df
                # Clustering summary statistics:
                .transform(
                    lambda df: WindowBasedClumping._cluster_sumstats(
                        df, window_length, p_value_significance
                    )
                )
                # Collect locus data for snps reacing significance:
                .withColumn(
                    "locus",
                    f.when(
                        filter_expression,
                        f.collect_set(
                            f.struct(
                                f.col("variantId"),
                                f.col("pValueMantissa"),
                                f.col("pValueExponent"),
                                f.col("beta"),
                            )
                        ).over(locus_window),
                    ).otherwise(None),
                )
                # Dropping non-significant spns:
                .filter(filter_expression)
                # Rank hits in cluster:
                .withColumn("pvRank", f.row_number().over(cluster_window))
                # Collect positions in cluster for the first
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
                # Get leads:
                .withColumn(
                    "semiIndices",
                    f.when(
                        f.size(f.col("collectedPositions")) > 0,
                        fml.vector_to_array(
                            WindowBasedClumping._find_peak(
                                fml.array_to_vector(f.col("collectedPositions")),
                                f.lit(window_length),
                            )
                        ),
                    ),
                )
                .withColumn(
                    "semiIndices",
                    f.when(
                        f.col("semiIndices").isNull(),
                        f.first(f.col("semiIndices"), ignorenulls=True).over(
                            cluster_window
                        ),
                    ).otherwise(f.col("semiIndices")),
                )
                .filter(f.col("semiIndices").getItem(f.col("pvRank") - 1) > 0)
                .drop("pvRank", "collectedPositions", "semiIndices", "cluster_id")
                # Adding study-locus id:
                .withColumn(
                    "studyLocusId",
                    StudyLocus.assign_study_locus_id("studyId", "variantId"),
                )
            ),
            _schema=StudyLocus.get_schema(),
        )
