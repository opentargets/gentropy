"""This module provides toolset to clump together signals with overlapping ld set."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from etl.common.spark_helpers import adding_quality_flag
from etl.gwas_ingest.pics import _neglog_p

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

ASSOCIATION_EXPLAINED = "Explained by a more significant variant in high LD"


def clumping(df: DataFrame) -> DataFrame:
    """Clump non-independent credible sets.

    Args:
        df (DataFrame): The LD expanded dataset, before PICS calculation

    Returns:
        DataFrame: Clumped signals are resolved by:
            - removing tagging varinants of non independent leads.
            - removing overall R from non independent leads.
            - Adding QC flag to non-independent leads pointing to the relevant lead.
    """
    # GraphFrames needs this:
    w = Window.partitionBy("studyId", "variantPair").orderBy(f.col("negLogPVal").desc())

    # This dataframe contains all the resolved and independent leads. However not all linked signals are properly assigned to a more significant lead:
    resolved_independent = (
        df
        # lead/tag pairs irrespective of the order:
        .withColumn(
            "variantPair",
            f.concat_ws(
                "_", f.array_sort(f.array(f.col("variantId"), f.col("tagVariantId")))
            ),
        )
        # Generating the negLogPval:
        .withColumn(
            "negLogPVal", _neglog_p(f.col("pValueMantissa"), f.col("pValueExponent"))
        )
        # Counting the number of occurrence for each pair - for one study, each pair should occure only once:
        .withColumn("rank", f.row_number().over(w))
        # Only the most significant lead is kept if credible sets are overlapping + keeping lead if credible set is not resolved:
        .withColumn(
            "keep_lead",
            f.when(
                f.max(f.col("rank")).over(Window.partitionBy("studyId", "variantId"))
                <= 1,
                True,
            ).otherwise(False),
        )
        # Generate QC notes for explained associations:
        .withColumn(
            "qualityControl",
            adding_quality_flag(
                f.col("qualityControl"),
                ~f.col("keep_lead"),
                f.lit(ASSOCIATION_EXPLAINED),
            ),
        )
        # Remove tag information if lead is explained by other variant:
        .withColumn(
            "tagVariantId",
            f.when(f.col("keep_lead"), f.col("tagVariantId")),
        )
        .withColumn("R_overall", f.when(f.col("keep_lead"), f.col("R_overall")))
        # Drop unused column:
        .drop(
            "variantPair",
            "negLogPVal",
            "rank",
            "keep_lead",
        )
        .distinct()
        .orderBy("studyId", "variantId")
    )

    return resolved_independent
