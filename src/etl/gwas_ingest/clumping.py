"""This module provides toolset to clump together signals with overlapping ld set."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from etl.gwas_ingest.pics import _neglog_p

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


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
        # Adding reference lead that explains:
        .withColumn(
            "reference_lead",
            f.when(f.col("rank") > 1, f.col("tagVariantId")).when(
                f.col("keep_lead"), f.col("variantId")
            ),
        )
        # One credible set might contain multiple tags that are themselves leads. We need to collect them into an array:
        # At this point we move from lead/tag to study/lead level:
        .withColumn(
            "all_explained",
            f.collect_set(f.col("reference_lead")).over(
                Window.partitionBy("studyId", "variantId")
            ),
        )
        .withColumn("explained", f.explode("all_explained"))
        .drop("reference_lead", "all_explained")
        .persist()
    )

    # Test
    return resolved_independent
