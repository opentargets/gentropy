"""LD annotation for studyLocus."""
import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus

WINDOW_SIZE = 500_000
NUM_STUDIES = 500

session = Session(
    spark_uri="yarn",
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "3200",
    },
)

# Read LD information
ld = (
    session.spark.read.parquet(
        "gs://ot-team/dochoa/study_locus_expl_ld_27_02_2024.parquet"
    )
    .withColumn("position_i", f.split(f.col("variantId_i"), "_")[1].cast("int"))
    .withColumn("position_j", f.split(f.col("variantId_j"), "_")[1].cast("int"))
)

# Summary statistics from GCST006907 (not so big study)

sl = StudyLocus.from_parquet(session, path="gs://ot-team/dochoa/sl_11_3_24.parquet")

(
    ld.alias("ld")
    .join(
        f.broadcast(
            sl.df.alias("sl")
            .select(
                "studyLocusId",
                "chromosome",
                "position",
                # f.col("locus.variantId").alias("variantIdArray"),
            )
            .repartitionByRange("chromosome", "position")
            .sortWithinPartitions("chromosome", "position")
        ),
        on=[
            f.col("sl.chromosome") == f.col("ld.chromosome"),
            f.col("position_i") > (f.col("sl.position") - WINDOW_SIZE),
            f.col("position_i") < (f.col("sl.position") + WINDOW_SIZE),
            f.col("position_j") > (f.col("sl.position") - WINDOW_SIZE),
            f.col("position_j") < (f.col("sl.position") + WINDOW_SIZE),
            # f.array_contains("variantIdArray", f.col("variantId_i")),
            # f.array_contains("variantIdArray", f.col("variantId_j")),
        ],
    )
    .select(
        "studyLocusId",
        f.col("variantId_i"),
        f.col("variantId_j"),
        f.col("r"),
    )
    .write.partitionBy("studyLocusId")
    .parquet("gs://ot-team/dochoa/ldmatrixes_21_3_24.parquet", mode="overwrite")
)
