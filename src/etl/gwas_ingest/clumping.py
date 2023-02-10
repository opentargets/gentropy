"""This module provides toolset to clump together signals with overlapping ld set."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from graphframes import GraphFrame
from graphframes.lib import Pregel
from pyspark.sql import Window

from etl.common.spark_helpers import adding_quality_flag
from etl.gwas_ingest.pics import _neglog_p

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def resolve_graph(df: DataFrame) -> DataFrame:
    """Graph resolver for clumping.

    It takes a dataframe with a list of variants and their explained variants, and returns a dataframe
    with a list of variants and their resolved roots

    Args:
        df (DataFrame): DataFrame

    Returns:
        A dataframe with the resolved roots.
    """
    # Convert to vertices:
    nodes = df.select(
        "studyId",
        "variantId",
        # Generating node identifier column (has to be unique):
        f.concat_ws("_", f.col("studyId"), f.col("variantId")).alias("id"),
        # Generating the original root list. This is the original message which is propagated across nodes:
        f.when(f.col("variantId") == f.col("explained"), f.col("variantId")).alias(
            "origin_root"
        ),
    ).distinct()

    # Convert to edges (more significant points to less significant):
    edges = (
        df.filter(f.col("variantId") != f.col("explained"))
        .select(
            f.concat_ws("_", f.col("studyId"), f.col("variantId")).alias("dst"),
            f.concat_ws("_", f.col("studyId"), f.col("explained")).alias("src"),
            f.lit("explains").alias("edgeType"),
        )
        .distinct()
    )

    # Building graph:
    graph = GraphFrame(nodes, edges)

    # Extracing nodes with edges - most of the
    filtered_nodes = (
        graph.outDegrees.join(graph.inDegrees, on="id", how="outer")
        .drop("outDegree", "inDegree")
        .join(nodes, on="id", how="inner")
        .repartition("studyId", "variantId")
    )

    # Building graph:
    graph = GraphFrame(filtered_nodes, edges)

    # Pregel resolver:
    resolved_nodes = (
        graph.pregel.setMaxIter(5)
        # New column for the resolved roots:
        .withVertexColumn(
            "message",
            f.when(f.col("origin_root").isNotNull(), f.col("origin_root")),
            f.when(Pregel.msg().isNotNull(), Pregel.msg()),
        )
        .withVertexColumn(
            "resolved_roots",
            # The value is initialized by the original root value:
            f.when(
                f.col("origin_root").isNotNull(), f.array(f.col("origin_root"))
            ).otherwise(f.array()),
            # When new value arrives to the node, it gets merged with the existing list:
            f.when(
                Pregel.msg().isNotNull(),
                f.array_union(f.split(Pregel.msg(), " "), f.col("resolved_roots")),
            ).otherwise(f.col("resolved_roots")),
        )
        # We need to reinforce the message in both direction:
        .sendMsgToDst(Pregel.src("message"))
        # Once the message is delivered it is updated with the existing list of roots at the node:
        .aggMsgs(f.concat_ws(" ", f.collect_set(Pregel.msg())))
        .run()
        .orderBy("studyId", "id")
        .persist()
    )

    # Joining back the dataset:
    return df.join(
        # The `resolved_roots` column will be null for nodes, with no connection.
        resolved_nodes.select("resolved_roots", "studyId", "variantId"),
        on=["studyId", "variantId"],
        how="left",
    )


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
        .transform(resolve_graph)
        # Generate QC notes for explained associations:
        .withColumn(
            "qualityControl",
            adding_quality_flag(
                f.col("qualityControl"),
                ~f.col("keep_lead"),
                f.concat_ws(
                    " ",
                    f.lit("Association explained by:"),
                    f.concat_ws(", ", f.col("resolved_roots")),
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
            "resolved_roots",
        )
        .distinct()
        .orderBy("studyId", "variantId")
    )

    # Test
    return resolved_independent
