"""Exploring explosion of the LD matrix to allow finemapping."""

## Machine
# gcloud dataproc clusters create do-test-ld \
#         --image-version=2.1 \
#         --project=open-targets-genetics-dev \
#         --region=europe-west1 \
#         --zone="" \
#         --master-machine-type=n1-highmem-8 \
#         --autoscaling-policy="do-ld-explosion" \
#         --enable-component-gateway \
#         --initialization-actions=gs://genetics_etl_python_playground/initialisation/0.0.0/install_dependencies_on_cluster.sh \
# 		--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/0.0.0/gentropy-0.0.0-py3-none-any.whl,CONFIGTAR=gs://genetics_etl_python_playground/initialisation/0.0.0/config.tar.gz" \
#         --secondary-worker-boot-disk-size=100GB \
#         --worker-boot-disk-size=2000GB \
#         --num-workers=4 \
#         --worker-machine-type=n1-highmem-8


import pyspark.sql.functions as f
from clump_temp import WindowBasedClumping
from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.summary_statistics import SummaryStatistics

WINDOW_SIZE = 500_000
NUM_STUDIES = 500

session = Session(
    spark_uri="yarn",
    extended_spark_conf={
        "spark.sql.shuffle.partitions": str(NUM_STUDIES),
    },
)

# NFE GWAS Catalog: total 52_081 out of 80_587 of GWAS Catalog
# TODO: add filter by % of NFE individuals
nfe_studies = StudyIndex.from_parquet(
    session,
    "gs://genetics_etl_python_playground/releases/24.01/study_index/gwas_catalog/",
).df.filter(
    f.array_contains(
        "ldPopulationStructure",
        f.struct(
            f.lit("nfe").alias("ldPopulation"), f.lit(1.0).alias("relativeSampleSize")
        ),
    )
)

# 13531 out of 15987 of GWAS Catalog summary statistics are ONLY NFE
studies = nfe_studies.join(
    session.spark.read.parquet(
        "gs://gwas_catalog_data/manifests/gwas_catalog_summary_statistics_included_studies/"
    ),
    on="studyId",
)
study_list = studies.rdd.map(lambda x: x.studyId).collect()

to_do_list = [
    "gs://gwas_catalog_data/harmonised_summary_statistics/" + i + ".parquet"
    for i in study_list
][1:NUM_STUDIES]
ss = SummaryStatistics.from_parquet(
    session,
    path=to_do_list,
).exclude_region("6:28510120-33480577")

ss.df.repartition(NUM_STUDIES, "studyId", "chromosome").sortWithinPartitions(
    "studyId", "chromosome", "position"
)

# StudyLocus count in all NFE GWAS Catalog summary statistics: 154_715
sl = WindowBasedClumping.window_based_clumping(
    ss, distance=WINDOW_SIZE, collect_locus=True, collect_locus_distance=WINDOW_SIZE
)

sl.df.write.parquet(
    path="gs://ot-team/dochoa/sl_collect_500_28_02_2024.parquet",
    mode="overwrite",
)
