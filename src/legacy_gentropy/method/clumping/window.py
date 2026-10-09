"""Clumps GWAS significant variants from summary statistics with a distance based method."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Self

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pydantic import BaseModel, Field
from pyspark.ml import functions as fml
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql.window import Window

from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityFlag
from gentropy.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from numpy.typing import NDArray
    from pyspark.sql import Column


class WindowClumpingConfig(BaseModel):
    """Window Based Clumping configuration."""

    distance: Annotated[int, Field(gt=0)] = 500_000
    """Distance within which tagging variants are collected around the semi-index."""
    gwas_significance: Annotated[float, Field(gt=0, lt=1)] = 5e-8
    """GWAS significance."""


class WindowClumping:
    """Get semi-lead snps from summary statistics using a window based function."""

    def __init__(self: Self, distance: int, gwas_significance: float) -> None:
        """Initialize the method with parameters."""
        self.config = WindowClumpingConfig(
            distance=distance,
            gwas_significance=gwas_significance,
        )

    def clump(self: Self, summary_statistics: SummaryStatistics) -> StudyLocus:
        """Clump single point associations from summary statistics or study locus dataset based on window.

        Args:
            summary_statistics (SummaryStatistics): Input dataset to be used for clumping. Assumes that the input dataset is already filtered for significant variants.

        Returns:
            StudyLocus: clumped associations, where the clumped variants are flagged.
        """
        # The quality control expression will depend on the input dataset, as the column might be already present:
        qc_expression = f.when(
            f.col("semiIndices")[f.col("pvRank") - 1] <= 0,
            f.array(f.lit(StudyLocusQualityFlag.WINDOW_CLUMPED.value)),
        ).otherwise(f.array().cast(t.ArrayType(t.StringType())))

        # Create window for locus clusters
        # - variants where the distance between subsequent variants is below the defined threshold.
        # - Variants are sorted by descending significance
        cluster_window = Window.partitionBy(
            "studyId",
            "chromosome",
            "clusterId",
        ).orderBy(
            f.col("pValueExponent").asc(),
            f.col("pValueMantissa").asc(),
        )

        return StudyLocus(
            _df=(
                summary_statistics.df
                # Clustering variants for efficient windowing (complexity reduction):
                .select(
                    f.col("studyId"),
                    f.col("chromosome"),
                    f.split("variantId", "_").getField(1).alias("position"),
                    f.col("variantId"),
                    f.col("pValueMantissa"),
                    f.col("pValueExponent"),
                )
                .withColumn(
                    "clusterId",
                    self._cluster_peaks(
                        f.col("studyId"),
                        f.col("chromosome"),
                        f.col("position"),
                        self.config.distance,
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
                # Collect top loci per cluster:
                .withColumn(
                    "semiIndices",
                    f.when(
                        f.size(f.col("collectedPositions")) > 0,
                        fml.vector_to_array(
                            f.udf(self._prune_peak, VectorUDT())(
                                fml.array_to_vector(f.col("collectedPositions")),
                                f.lit(self.config.distance),
                            )
                        ),
                    ),
                )
                .drop("collectedPositions")
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
                .drop("clusterId")
                # Add locusStart & locusEnd
                .withColumns(
                    {
                        "locusStart": f.greatest(
                            f.lit(0), f.col("position") - f.lit(self.config.distance)
                        ),
                        "locusEnd": f.col("position") + f.lit(self.config.distance),
                    }
                )
                # Adding study-locus id:
                .withColumn(
                    "studyLocusId",
                    StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS),
                )
                # Initialize QC column as array of strings:
                .withColumn("qualityControls", qc_expression)
                .drop("pvRank", "semiIndices")
            ),
            _schema=StudyLocus.get_schema(),
        )

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
    def _prune_peak(position: NDArray[np.float64], window_size: int) -> DenseVector:
        """Establish lead snps based on their positions listed by p-value.

        The function `find_peak` assigns lead SNPs based on their positions listed by p-value within a specified window size.

        Args:
            position (NDArray[np.float64]): positions of the SNPs sorted by p-value.
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
        is_lead = np.zeros(len(position))

        # List containing indices of leads:
        lead_indices: list[int] = []

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
