"""Find overlapping signals susceptible of colocalisation analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def find_all_vs_all_overlapping_signals(
    credible_sets_enriched: DataFrame,
) -> DataFrame:
    """Find overlapping signals.

    Find overlapping signals between all pairs of cred sets (exploded at the tag variant level)
    Any study-lead variant pair with at least one overlapping tag variant is considered.

    All GWAS-GWAS and all GWAS-Molecular traits are computed with the Molecular traits always
    appearing on the right side.

    Args:
        credible_sets_enriched (DataFrame): DataFrame containing the credible sets to be analysed

    Returns:
        DataFrame: overlapping peaks to be compared
    """
    # Columnns to be used as left and right
    id_cols = [
        "chromosome",
        "studyId",
        "leadVariantId",
        "type",
    ]
    metadata_cols = ["phenotype", "biofeature", "gene_id"]

    # Self join with complex condition. Left it's all gwas and right can be gwas or molecular trait
    # This function includes some metadata about the overlap that needs to be dropped to avoid duplicates
    cols_to_drop = ["left_logABF", "right_logABF", "tagVariantId"]
    overlapping_peaks = (
        find_gwas_vs_all_overlapping_peaks(credible_sets_enriched, "logABF")
        # Keep overlapping peaks where logABF is at either side
        .filter(
            f.col("left_logABF").isNotNull() & f.col("right_logABF").isNotNull()
        ).drop(*cols_to_drop)
    )

    # Bring metadata from left and right for all variants that tag the peak
    overlapping_left = credible_sets_enriched.selectExpr(
        [f"{col} as left_{col}" for col in id_cols + metadata_cols + ["logABF"]]
        + ["tagVariantId"]
    ).join(
        overlapping_peaks.sortWithinPartitions([f"left_{i}" for i in id_cols]),
        on=[f"left_{i}" for i in id_cols],
        how="inner",
    )

    overlapping_right = credible_sets_enriched.selectExpr(
        [f"{col} as right_{col}" for col in id_cols + metadata_cols + ["logABF"]]
        + ["tagVariantId"]
    ).join(
        overlapping_peaks.sortWithinPartitions([f"right_{i}" for i in id_cols]),
        on=[f"right_{i}" for i in id_cols],
        how="inner",
    )

    return overlapping_left.join(
        overlapping_right,
        on=[f"right_{i}" for i in id_cols]
        + [f"left_{i}" for i in id_cols]
        + ["tagVariantId"],
        how="outer",
    )


def find_gwas_vs_all_overlapping_peaks(
    credible_sets_enriched: DataFrame, causality_statistic: str
) -> DataFrame:
    """Find overlapping signals between GWAS (left) and GWAS or Molecular traits (right).

    The main principle is that here we extract which are the peaks that share a tagging variant for the same trait.

    Args:
        credible_sets_enriched (DataFrame): DataFrame containing the credible sets to be analysed
        causality_statistic (str): Causality statistic to be used for the analysis. Can be either logABF or posteriorProbability

    Returns:
        DataFrame: overlapping peaks to be compared
    """
    id_cols = [
        "chromosome",
        "studyId",
        "leadVariantId",
        "type",
    ]
    cols_to_rename = id_cols + [causality_statistic]
    credset_to_self_join = credible_sets_enriched.select(
        id_cols + ["tagVariantId", causality_statistic]
    )
    return (
        credset_to_self_join.alias("left")
        .filter(f.col("type") == "gwas")
        .join(
            credset_to_self_join.alias("right"),
            on=[
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                (f.col("right.type") != "gwas")
                | (f.col("left.studyId") > f.col("right.studyId")),
            ],
            how="inner",
        )
        # Rename columns to make them unambiguous
        .selectExpr(
            *(
                [f"left.{col} as left_{col}" for col in cols_to_rename]
                + [f"right.{col} as right_{col}" for col in cols_to_rename]
                + ["left.tagVariantId as tagVariantId"],
            )
        )
        .dropDuplicates(
            [f"left_{i}" for i in id_cols] + [f"right_{i}" for i in id_cols]
        )
        .cache()
    )
