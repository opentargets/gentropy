import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.ml.functions as Fml


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
        credSet
        # Exclude studies without logABFs available
        .filter(F.col("logABF").isNotNull())
        .withColumn(
            "all_tags",
            F.map_from_entries(
                F.collect_list(F.struct("tag_variant_id", "logABF")).over(wind1)
            ),
        )
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
        .drop("left.tag_variant_id", "right.tag_variant_id")
    )

    # Rename columns in left and right to make them unambiguous
    colsToRename = idCols + metadataCols + ["all_tags"]
    selectExpr = ["left." + col + " as " + "left_" + col for col in colsToRename] + [
        "right." + col + " as " + "right_" + col for col in colsToRename
    ]
    overlappingPeaks = (
        overlappingTags.selectExpr(
            *selectExpr
        )  # Keep only one record per overlapping peak
        # TODO: think whether this could be optimised
        .dropDuplicates(["left_" + i for i in idCols] + ["right_" + i for i in idCols])
    )

    # Exctract logABF vectors for each vector mapped by tag_variant_id
    overlappingPeaksWithArrays = (
        overlappingPeaks.withColumn(
            "left_logABF",
            Fml.array_to_vector(
                F.map_values(
                    F.map_zip_with(
                        "left_all_tags",
                        "right_all_tags",
                        lambda k, v1, v2: F.coalesce(v1, F.lit(0.0)),
                    )
                )
            ),
        )
        .withColumn(
            "right_logABF",
            Fml.array_to_vector(
                F.map_values(
                    F.map_zip_with(
                        "left_all_tags",
                        "right_all_tags",
                        lambda k, v1, v2: F.coalesce(v2, F.lit(0.0)),
                    )
                )
            ),
        )
        .drop("right_all_tags", "left_all_tags")
    )

    return overlappingPeaksWithArrays
