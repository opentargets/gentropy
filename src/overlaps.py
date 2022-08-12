"""Find overlapping signals susceptible of colocalisation analysis
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def find_all_vs_all_overlapping_signals(spark: SparkSession, credset_path: str):
    """
    Find overlapping signals between all pairs of cred sets (exploded at the tag variant level)
    Any study-lead variant pair with at least one overlapping tag variant is considered

    Args:
        spark: SparkSession
        credset_path: Path to credible sets

    """

    # Columnns to be used as left and right
    id_cols = [
        "chrom",
        "studyKey",
        "lead_variant_id",
        "type",
    ]
    metadata_cols = [
        "study_id",
        "phenotype_id",
        "bio_feature",
        "lead_chrom",
        "lead_pos",
        "lead_ref",
        "lead_alt",
    ]

    credset = (
        spark.read.parquet(credset_path)
        # .filter(F.col("chrom") == "22") # for debugging
        .withColumn(
            "studyKey",
            F.xxhash64(*["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        # Exclude studies without logABFs available
        .filter(F.col("logABF").isNotNull())
    )

    # Self join with complex condition. Left it's all gwas and right can be gwas or molecular trait
    cols_to_rename = id_cols
    credset_to_self_join = credset.select(id_cols + ["tag_variant_id"])
    overlapping_peaks = (
        credset_to_self_join.alias("left")
        .filter(F.col("type") == "gwas")
        .join(
            credset_to_self_join.alias("right"),
            on=[
                F.col("left.chrom") == F.col("right.chrom"),
                F.col("left.tag_variant_id") == F.col("right.tag_variant_id"),
                (F.col("right.type") != "gwas")
                | (F.col("left.studyKey") > F.col("right.studyKey")),
            ],
            how="inner",
        )
        .drop("left.tag_variant_id", "right.tag_variant_id")
        # Rename columns to make them unambiguous
        .selectExpr(
            *["left." + col + " as " + "left_" + col for col in cols_to_rename]
            + ["right." + col + " as " + "right_" + col for col in cols_to_rename]
        )
        # Keep only one record per overlapping peak
        .dropDuplicates(
            ["left_" + i for i in id_cols] + ["right_" + i for i in id_cols]
        )
        .cache()
    )

    overlapping_left = credset.selectExpr(
        [col + " as " + "left_" + col for col in id_cols + metadata_cols + ["logABF"]]
        + ["tag_variant_id"]
    ).join(
        overlapping_peaks.sortWithinPartitions(["left_" + i for i in id_cols]),
        on=["left_" + i for i in id_cols],
        how="inner",
    )

    overlapping_right = credset.selectExpr(
        [col + " as " + "right_" + col for col in id_cols + metadata_cols + ["logABF"]]
        + ["tag_variant_id"]
    ).join(
        overlapping_peaks.sortWithinPartitions(["right_" + i for i in id_cols]),
        on=["right_" + i for i in id_cols],
        how="inner",
    )

    overlapping_signals = overlapping_left.join(
        overlapping_right,
        on=["right_" + i for i in id_cols]
        + ["left_" + i for i in id_cols]
        + ["tag_variant_id"],
        how="outer",
    )

    return overlapping_signals
