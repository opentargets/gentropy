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
    credSet = spark.read.parquet(credSetPath).withColumn(
        "studyKey",
        F.concat_ws("_", *["type", "study_id", "phenotype_id", "bio_feature"]),
    )

    # Columnns to be used as left and right
    idCols = [
        "studyKey",
        "lead_variant_id",
        "type",
    ]
    metadataCols = [
        "study_id",
        "phenotype_id",
        "bio_feature",
        "lead_chrom",
        "lead_pos",
        "lead_ref",
        "lead_alt",
    ]

    leftDf = reduce(
        lambda DF, col: DF.withColumnRenamed(col, "left_" + col),
        idCols + metadataCols + ["logABF"],
        credSet.select(idCols + metadataCols + ["logABF", "tag_variant_id"]),
    )
    rightDf = reduce(
        lambda DF, col: DF.withColumnRenamed(col, "right_" + col),
        idCols + metadataCols + ["logABF"],
        credSet.select(idCols + metadataCols + ["logABF", "tag_variant_id"]),
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
        # drop unnecesarry data for finding overlapping signals
        .select(
            *["left_" + col for col in idCols] + ["right_" + col for col in idCols]
        ).distinct()
    )

    overlappingLeft = overlappingPeaks.join(
        leftDf,
        on=["left_studyKey", "left_lead_variant_id", "left_type"],
        how="inner",
    )
    overlappingRight = overlappingPeaks.join(
        rightDf,
        on=["right_studyKey", "right_lead_variant_id", "right_type"],
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
