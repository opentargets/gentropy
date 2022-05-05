from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def findOverlappingSignals(spark: SparkSession, credSetPath: str):
    """
    Find overlapping signals between all pairs of cred sets (exploded at the tag variant level)

    Args:
        spark: SparkSession
        credSetPath: Path to credible sets

    Returns:
        DataFrame with columns:
            # gene1: Gene 1
            # gene2: Gene 2
            # overlap: Number of overlapping signals
    """
    credSet = (
        spark.read.parquet(credSetPath)
        .distinct()
        .withColumn(
            "studyKey",
            F.concat_ws("_", *["type", "study_id", "phenotype_id", "bio_feature"]),
        )
    )

    # Columnns to be used as left and right
    renameColumns = [
        "studyKey",
        "lead_variant_id",
        "type",
        "logABF",
        "study_id",
        "phenotype_id",
        "bio_feature",
        "lead_chrom",
        "lead_pos",
        "lead_ref",
        "lead_alt",
    ]
    # All columns to be used
    columnsToJoin = renameColumns + ["tag_variant_id"]

    leftDf = reduce(
        lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
        renameColumns,
        credSet.select(columnsToJoin).distinct(),
    )
    rightDf = reduce(
        lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
        renameColumns,
        credSet.select(columnsToJoin).distinct(),
    )

    overlappingPeaks = (
        leftDf
        # molecular traits always on the right-side
        .filter(F.col("left_type") == "gwas")
        # Get all study/peak pairs where at least one tagging variant overlap:
        .join(rightDf, on="tag_variant_id", how="inner")
        # Different study keys
        .filter(
            # Remove rows with identical study:
            (F.col("left_studyKey") != F.col("right_studyKey"))
        )
        # Keep only the upper triangle where both study is gwas
        .filter(
            (F.col("right_type") != "gwas")
            | (F.col("left_studyKey") > F.col("right_studyKey"))
        )
        # remove overlapping tag variant info
        .drop("left_logABF", "right_logABF", "tag_variant_id")
        # distinct to get study-pair info
        .distinct()
    )

    overlappingLeft = overlappingPeaks.join(
        leftDf.select(
            "left_studyKey", "left_lead_variant_id", "tag_variant_id", "left_logABF"
        ),
        on=["left_studyKey", "left_lead_variant_id"],
        how="inner",
    )
    overlappingRight = overlappingPeaks.select(
        "right_studyKey",
        "right_lead_variant_id",
        "right_type",
        "left_studyKey",
        "left_lead_variant_id",
        "left_type",
    ).join(
        rightDf.select(
            "right_studyKey", "right_lead_variant_id", "tag_variant_id", "right_logABF"
        ),
        on=["right_studyKey", "right_lead_variant_id"],
        how="inner",
    )

    overlappingSignals = overlappingLeft.alias("a").join(
        overlappingRight.alias("b"),
        on=[
            "tag_variant_id",
            "left_lead_variant_id",
            "right_lead_variant_id",
            "left_studyKey",
            "right_studyKey",
            "right_type",
            "left_type",
        ],
        how="outer",
    )
    return overlappingSignals
