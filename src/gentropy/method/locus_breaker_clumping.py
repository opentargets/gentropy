"""Locus-breaker clumping method."""

from __future__ import annotations

import sys

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

from gentropy.common.stats import neglogpval_from_pvalue
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics


class LocusBreakerClumping:
    """Locus-breaker clumping method."""

    @staticmethod
    def locus_breaker(
        summary_statistics: SummaryStatistics,
        baseline_pvalue_cutoff: float,
        distance_cutoff: int,
        pvalue_cutoff: float,
        flanking_distance: int,
    ) -> StudyLocus:
        """Identify GWAS associated loci based on the provided p-value and distance cutoff.

        - The GWAS associated loci identified by this method have a varying width, and are separated by a distance greater than the provided distance cutoff.
        - The distance is only calculted between single point associations that reach the baseline p-value cutoff.
        - As the width of the selected genomic region dynamically depends on the loci, the resulting StudyLocus object will contain the locus start and end position.
        - To ensure completeness, the locus is extended by a flanking distance in both ends.

        Args:
            summary_statistics (SummaryStatistics): Input summary statistics dataset.
            baseline_pvalue_cutoff (float): baseline significance we consider for the locus.
            distance_cutoff (int): minimum distance that separates two loci.
            pvalue_cutoff (float): the minimum significance the locus should have.
            flanking_distance (int): the distance to extend the locus in both directions.

        Returns:
            StudyLocus: clumped study loci with locus start and end positions + lead variant from the locus.
        """
        # Convert pvalue_cutoff to neglog scale:
        neglog_pv_cutoff = -np.log10(pvalue_cutoff)

        # First window to calculate the distance between consecutive positions:
        w1 = Window.partitionBy("studyId", "chromosome").orderBy("position")

        # Second window to calculate the locus start and end:
        w2 = (
            Window.partitionBy("studyId", "chromosome", "locusStart")
            .orderBy("position")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        # Third window to rank the variants within the locus based on neglog p-value to find top loci:
        w3 = Window.partitionBy(
            "studyId", "chromosome", "locusStart", "locusEnd"
        ).orderBy(f.col("negLogPValue").desc())

        clumped = (
            # Applying the baseline p-value cutoff:
            summary_statistics.pvalue_filter(baseline_pvalue_cutoff)
            .df.select(
                "studyId",
                "variantId",
                "chromosome",
                "position",
                # Calculating the neglog p-value for easier sorting:
                neglogpval_from_pvalue(
                    f.col("pValueMantissa"), f.col("pValueExponent")
                ).alias("negLogPValue"),
            )
            # Calculating the distance between consecutive positions, then identifying the locus start and end:
            .withColumn("next_position", f.lag(f.col("position")).over(w1))
            .withColumn("distance", f.col("position") - f.col("next_position"))
            .withColumn(
                "locusStart",
                f.when(
                    (f.col("distance") > distance_cutoff) | f.col("distance").isNull(),
                    f.col("position"),
                ),
            )
            .withColumn(
                "locusStart",
                f.when(
                    f.last(f.col("locusStart") - flanking_distance, True).over(
                        w1.rowsBetween(-sys.maxsize, 0)
                    )
                    > 0,
                    f.last(f.col("locusStart") - flanking_distance, True).over(
                        w1.rowsBetween(-sys.maxsize, 0)
                    ),
                ).otherwise(f.lit(0)),
            )
            .withColumn(
                "locusEnd", f.max(f.col("position") + flanking_distance).over(w2)
            )
            .withColumn("rank", f.rank().over(w3))
            .filter((f.col("rank") == 1) & (f.col("negLogPValue") > neglog_pv_cutoff))
            .select(
                StudyLocus.assign_study_locus_id(["studyId", "variantId"]),
                f.col("studyId"),
                f.col("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.lit(None).cast(t.ArrayType(t.StringType())).alias("qualityControls"),
                # To make sure that the type of locusStart and locusEnd follows schema of StudyLocus:
                f.col("locusStart").cast(t.IntegerType()).alias("locusStart"),
                f.col("locusEnd").cast(t.IntegerType()).alias("locusEnd"),
            )
        )
        return StudyLocus(
            _df=clumped,
            _schema=StudyLocus.get_schema(),
        )

    @staticmethod
    def merge_lbc_with_wbc_for_large_loci(
        lbc: StudyLocus,
        wbc: StudyLocus,
        large_loci_size: int,
    ) -> StudyLocus:
        """Merge LBC and WBC results, replacing large LBC loci with fixed-width WBC windows.

        Small LBC loci (span ≤ large_loci_size) are kept as-is. Large LBC loci are dropped
        and replaced by the WBC leads whose positions fall within those large loci boundaries,
        each assigned a fixed-width window of large_loci_size centred on the lead position.

        Args:
            lbc (StudyLocus): StudyLocus from locus-breaker clumping.
            wbc (StudyLocus): StudyLocus from window-based clumping (run on the same sumstats).
            large_loci_size (int): Span threshold in base pairs above which an LBC locus is
                replaced by WBC leads. Also defines the fixed window half-width assigned to
                those WBC leads (position ± large_loci_size // 2).

        Returns:
            StudyLocus: Union of small LBC loci and re-windowed WBC leads from large LBC loci.
        """
        large_loci_size = int(large_loci_size)
        small_loci = lbc.filter(
            (f.col("locusEnd") - f.col("locusStart")) <= large_loci_size
        )
        large_loci = lbc.filter(
            (f.col("locusEnd") - f.col("locusStart")) > large_loci_size
        )
        large_loci_wbc = StudyLocus(
            wbc.df.alias("wbc")
            .join(
                large_loci.df.alias("ll"),
                (f.col("wbc.studyId") == f.col("ll.studyId"))
                & (f.col("wbc.chromosome") == f.col("ll.chromosome"))
                & (
                    f.col("wbc.position").between(
                        f.col("ll.locusStart"), f.col("ll.locusEnd")
                    )
                ),
                "semi",
            )
            .withColumns(
                {
                    "locusStart": f.col("position") - large_loci_size // 2,
                    "locusEnd": f.col("position") + large_loci_size // 2,
                }
            ),
            StudyLocus.get_schema(),
        )
        return StudyLocus(
            large_loci_wbc.df.unionByName(small_loci.df),
            StudyLocus.get_schema(),
        )
