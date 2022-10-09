"""Experiment to extract LD reference using hail."""
from __future__ import annotations

import hail as hl
import pyspark.sql.functions as f
import pyspark.sql.types as t
from hail.linalg import BlockMatrix
from pyspark import SparkConf
from pyspark.sql import SparkSession

# TODO: MAF threshold required for meaningful r2? probably yes maf > 0.005?

pop = "nfe"
version = "2.1.1"
common_only = True

ld_window = 5e5
min_r2 = 0.5

conf = (
    SparkConf()
    .set(
        "spark.jars",
        "/home/ochoa/.cache/pypoetry/virtualenvs/otgenetics-Izsk1Unf-py3.8/lib/python3.8/site-packages/hail/backend/hail-all-spark.jar",
    )
    .set(
        "spark.driver.extraClassPath",
        "/home/ochoa/.cache/pypoetry/virtualenvs/otgenetics-Izsk1Unf-py3.8/lib/python3.8/site-packages/hail/backend/hail-all-spark.jar",
    )
    .set("spark.executor.extraClassPath", "./hail-all-spark.jar")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
)
spark = (
    SparkSession.builder.master("yarn").config(conf=conf).appName("test").getOrCreate()
)
hl.init(sc=spark.sparkContext, default_reference="GRCh38")

# associations_path = "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_associations"
associations_path = "gs://ot-team/dochoa/gwas_catalog_associations"
# https://github.com/populationgenomics/ancestry/blob/69f05f4d0b20dcd1b0248582989580400717b950/scripts/hail_batch/calculate_ld/calculate_ld.py
toplocis = (
    spark.read.parquet(associations_path)
    .filter(f.col("ref").isNotNull() & f.col("alt").isNotNull())
    .filter(f.col("chr_id") == "21")
    .sort("chr_pos")
    .select(
        f.concat_ws(
            ":", f.concat(f.lit("chr"), "chr_id"), "chr_pos", "ref", "alt"
        ).alias("variant38"),
        f.concat(
            f.lit("chr"),
            f.col("chr_id"),
            f.lit(":"),
            f.when(f.col("chr_pos") - ld_window < 0, "START")
            .otherwise((f.col("chr_pos") - ld_window).cast(t.IntegerType()))
            .cast(t.StringType()),
            f.lit("-"),
            # hardcoded from rg38.lengths["chr21"]
            f.when(f.col("chr_pos") + ld_window > 46709983, "END")
            .otherwise((f.col("chr_pos") + ld_window).cast(t.IntegerType()))
            .cast(t.StringType()),
        ).alias("region"),
    )
).distinct()


rg37 = hl.get_reference("GRCh37")
rg38 = hl.get_reference("GRCh38")
rg38.add_liftover("gs://hail-common/references/grch38_to_grch37.over.chain.gz", rg37)


toplocis_ht = hl.Table.from_spark(toplocis)
# parse variants
toplocis_ht = toplocis_ht.transmute(
    **hl.parse_variant(toplocis_ht.variant38, reference_genome="GRCh38")
)
# variant liftover
toplocis_ht = toplocis_ht.annotate(locus37=hl.liftover(toplocis_ht.locus, "GRCh37"))
# parse regions
toplocis_ht = toplocis_ht.annotate(
    region38=hl.parse_locus_interval(toplocis_ht.region, "GRCh38")
)
# region liftover
toplocis_ht = toplocis_ht.annotate(region37=hl.liftover(toplocis_ht.region38, "GRCh37"))

# TODO: follow-up on liftover problems one-2-many, failed liftovers, etc.
toplocis_ht = toplocis_ht.key_by("locus37", "alleles", "region37")
toplocis_ht = toplocis_ht.filter(~hl.is_missing(toplocis_ht["region37"]))

bmpath = f"gs://gcp-public-data--gnomad/release/{version}/ld/gnomad.genomes.r{version}.{pop}.common.adj.ld.bm"
indexpath = f"gs://gcp-public-data--gnomad/release/{version}/ld/gnomad.genomes.r{version}.{pop}.common.adj.ld.variant_indices.ht/"

bm = BlockMatrix.read(bmpath)
ld_index = hl.read_table(indexpath)

# TODO: toplocis missing in LD reference will be dropped here
# TODO: toploci_ld_index could be contained insde region_ld_index
toploci_ld_index = (
    toplocis_ht.key_by("locus37", "alleles")
    .join(ld_index.key_by("locus", "alleles"))
    .add_index("i")
)
region_ld_index = (
    ld_index.filter(hl.is_defined(toplocis_ht.key_by("region37")[ld_index.locus]))
    .add_index("j")
    .persist()
)


# TODO: check _localize
start, stop = hl.linalg.utils.locus_windows(
    region_ld_index.locus, ld_window, _localize=False
)
start = start.collect()[0]
stop = stop.collect()[0]

# TODO: to review. see if collect can be avoided
toploci_idxs = toploci_ld_index.idx.collect()
region_idxs = region_ld_index.idx.collect()
# is_top_loci = [x in toploci_idxs for x in region_idxs]
toploci_idx_inregion = [
    idx for idx, value in enumerate(region_idxs) if value in toploci_idxs
]

# TODO: to explore
# blocks_only (bool) – If False, set all elements outside row intervals to zero. If True, only set all blocks outside row intervals to blocks of zeros; this is more efficient.
bm_sparse = bm.filter(toploci_idxs, region_idxs).sparsify_row_intervals(
    [start[i] for i in toploci_idx_inregion],
    [stop[i] for i in toploci_idx_inregion],
)

bm_r2 = bm_sparse**2

entries = bm_r2.entries()
entries = entries.filter(entries.entry >= min_r2)

region_ld_index = region_ld_index.key_by("j")
toploci_ld_index = toploci_ld_index.key_by("i")
results = (
    entries.key_by("j")
    .join(
        region_ld_index.select(
            region_variant37=region_ld_index.locus,
            region_alleles=region_ld_index.alleles,
        )
    )
    .key_by("i")
    .join(
        toploci_ld_index.select(
            toploci_variant=toploci_ld_index.locus,
            toploci_alleles=toploci_ld_index.alleles,
            toploci_variant37=toploci_ld_index.locus37,
        )
    )
    .key_by("toploci_variant")
    .drop("i", "j")
    .rename({"entry": "r2"})
)
