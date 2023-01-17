"""This module provides toolset to clump together signals with overlapping ld set."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from otg.common.spark_helpers import adding_quality_flag
from otg.gwas_ingest.pics import _neglog_p

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _get_resolver(df: DataFrame) -> DataFrame:
    """Getting a lead to more significant lead mapping for joining.

    Args:
        df (DataFrame): LD sets with some level of resolution

    Returns:
        DataFrame
    """
    return df.select(
        "studyId",
        f.col("variantId").alias("explained"),
        f.col("explained").alias("new_explained"),
    ).distinct()


def _clean_dataframe(df: DataFrame) -> DataFrame:
    """As part of the iterative lead variant resolve, come columns needs to be deleted.

    Args:
        df (DataFrame): Association dataset with resolved LD and R_overall calculated.

    Returns:
        DataFrame: Non-independent associations are assigned to the most significant linked lead
    """
    if "is_resolved" in df.columns:
        return df.drop("explained", "is_resolved").withColumnRenamed(
            "new_explained", "explained"
        )
    else:
        return df


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
        .withColumn("rank", f.count(f.col("tagVariantId")).over(w))
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

    # We need to keep iterating until all linked leads are resolved:
    while True:
        resolved_independent = (
            resolved_independent.transform(_clean_dataframe)
            .join(
                _get_resolver(resolved_independent),
                on=["studyId", "explained"],
                how="left",
            )
            .withColumn(
                "is_resolved",
                f.when(f.col("new_explained") == f.col("explained"), True).otherwise(
                    False
                ),
            )
            .persist()
        )

        if resolved_independent.select("is_resolved").distinct().count() == 1:
            break

    # At this point all linked leads are resolved. Now the dataframe needs to be consolidated:
    return (
        resolved_independent.drop("is_resolved", "new_explained", "neglog_pval")
        .withColumn(
            "qualityControl",
            adding_quality_flag(
                f.col("qualityControl"),
                ~f.col("keep_lead"),
                f.concat_ws(
                    " ",
                    f.lit("Association explained by:"),
                    f.concat_ws(
                        ", ",
                        f.collect_set(f.col("explained")).over(
                            Window.partitionBy("studyId", "variantId")
                        ),
                    ),
                ),
            ),
        )
        # Remove tag information if lead is explained by other variant:
        .withColumn(
            "tagVariantId",
            f.when(f.col("explained") == f.col("variantId"), f.col("tagVariantId")),
        )
        .withColumn(
            "R_overall", f.when(f.col("tagVariantId").isNotNull(), f.col("R_overall"))
        )
        # Drop unused column:
        .drop(
            "variantPair",
            "explained",
            "negLogPVal",
            "rank",
            "keep_lead",
        )
        .distinct()
        .orderBy("studyId", "variantId")
    )
