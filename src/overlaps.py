from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


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

    # Creating nested map with all tags in the study-lead variant
    wind1 = Window.partitionBy(idCols)
    credSetWithTagObjects = (
        credSet.withColumn("tag_object", F.create_map("tag_variant_id", "logABF"))
        .withColumn("all_tags", F.collect_list("tag_object").over(wind1))
        .select(idCols + metadataCols + ["tag_variant_id", "all_tags"])
    )

    # Self join with complex condition. Left it's all gwas and right can be gwas or molecular trait
    overlappingTags = (
        credSetWithTagObjects.alias("left")
        .filter(F.col("type") == "gwas")
        .join(
            credSetWithTagObjects.alias("right"),
            on=[
                F.col("left.tag_variant_id") == F.col("right.tag_variant_id"),
                (F.col("right.type") != "gwas")
                | (F.col("left.studyKey") > F.col("right.studyKey")),
            ],
            how="inner",
        )
    )

    # Drop multiple tags for the same peak pair
    wind2 = Window.partitionBy(
        ["left." + i for i in idCols] + ["right." + i for i in idCols]
    ).orderBy("left.tag_variant_id")

    overlappingPeaks = (
        overlappingTags.withColumn("row_number", F.row_number().over(wind2))
        .where(F.col("row_number") == 1)
        .drop("row_number", "left.tag_variant_id", "right.tag_variant_id")
    )

    # TODO: rename columns to make them unambiguous
    # TODO: new version of all_tags containing the nulls
