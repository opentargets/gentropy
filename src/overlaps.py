import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def findAllVsAllOverlappingSignals(spark: SparkSession, credSetPath: str):
    """
    Find overlapping signals between all pairs of cred sets (exploded at the tag variant level)

    Args:
        spark: SparkSession
        credSetPath: Path to credible sets

    """

    # Columnns to be used as left and right
    idCols = [
        "chrom",
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

    credSet = (
        spark.read.parquet(credSetPath)
        # TODO: for debugging
        # .filter(F.col("chrom") == "22")
        .withColumn(
            "studyKey",
            F.xxhash64(*["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        # Exclude studies without logABFs available
        .filter(F.col("logABF").isNotNull())
    )

    # Self join with complex condition. Left it's all gwas and right can be gwas or molecular trait
    colsToRename = idCols
    credSetToSelfJoin = credSet.select(idCols + ["tag_variant_id"])
    overlappingPeaks = (
        credSetToSelfJoin.alias("left")
        .filter(F.col("type") == "gwas")
        .join(
            credSetToSelfJoin.alias("right"),
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
            *["left." + col + " as " + "left_" + col for col in colsToRename]
            + ["right." + col + " as " + "right_" + col for col in colsToRename]
        )
        # Keep only one record per overlapping peak
        .dropDuplicates(["left_" + i for i in idCols] + ["right_" + i for i in idCols])
        .cache()
    )

    overlappingLeft = credSet.selectExpr(
        [col + " as " + "left_" + col for col in idCols + metadataCols + ["logABF"]]
        + ["tag_variant_id"]
    ).join(
        F.broadcast(overlappingPeaks.orderBy(["left_" + i for i in idCols])),
        on=["left_" + i for i in idCols],
        how="inner",
    )

    overlappingRight = credSet.selectExpr(
        [col + " as " + "right_" + col for col in idCols + metadataCols + ["logABF"]]
        + ["tag_variant_id"]
    ).join(
        F.broadcast(overlappingPeaks.orderBy(["right_" + i for i in idCols])),
        on=["right_" + i for i in idCols],
        how="inner",
    )

    overlappingSignals = overlappingLeft.join(
        overlappingRight,
        on=["right_" + i for i in idCols]
        + ["left_" + i for i in idCols]
        + ["tag_variant_id"],
        how="outer",
    )

    return overlappingSignals
