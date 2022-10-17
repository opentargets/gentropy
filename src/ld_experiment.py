"""Experiment to extract LD reference using hail."""
from __future__ import annotations

import hail as hl
import pyspark.sql.functions as f
import pyspark.sql.types as t
from hail.linalg import BlockMatrix
from pyspark import SparkConf
from pyspark.sql import SparkSession

pop = "nfe"
version = "2.1.1"
common_only = True

# LD-window size [(locus - ld_window) - (locus + ld_window)]
ld_window = 5e5
# Minimum r2 to be considered
min_r2 = 0.5

# Config to make hail-spark play nicely - this should be handled when the machine is setup
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

# Associations from GWAS catalog
# associations_path = "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_associations"
associations_path = "gs://ot-team/dochoa/gwas_catalog_associations"
# https://github.com/populationgenomics/ancestry/blob/69f05f4d0b20dcd1b0248582989580400717b950/scripts/hail_batch/calculate_ld/calculate_ld.py
# Extracting associations and region definitions
associations = (
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

# Hail datasets
# Liftover
rg37 = hl.get_reference("GRCh37")
rg38 = hl.get_reference("GRCh38")
rg38.add_liftover("gs://hail-common/references/grch38_to_grch37.over.chain.gz", rg37)

# Import associations to hail
associations_ht = hl.Table.from_spark(associations)

# parse variants
associations_ht = associations_ht.transmute(
    **hl.parse_variant(associations_ht.variant38, reference_genome="GRCh38")
)
# variant liftover
associations_ht = associations_ht.annotate(
    locus37=hl.liftover(associations_ht.locus, "GRCh37")
)
# parse regions
associations_ht = associations_ht.annotate(
    region38=hl.parse_locus_interval(associations_ht.region, "GRCh38")
)
# region liftover
associations_ht = associations_ht.annotate(
    region37=hl.liftover(associations_ht.region38, "GRCh37")
)

# TODO: follow-up on liftover problems one-2-many, failed liftovers, etc.
associations_ht = associations_ht.key_by("locus37", "alleles", "region37")
associations_ht = associations_ht.filter(~hl.is_missing(associations_ht["region37"]))

# From now on this is population-specific

# Block matrix
bmpath = f"gs://gcp-public-data--gnomad/release/{version}/ld/gnomad.genomes.r{version}.{pop}.common.adj.ld.bm"
# Index
indexpath = f"gs://gcp-public-data--gnomad/release/{version}/ld/gnomad.genomes.r{version}.{pop}.common.adj.ld.variant_indices.ht/"

# Read block matrix and indexes
bm = BlockMatrix.read(bmpath)
ld_index = hl.read_table(indexpath)

# TODO: handle the diagonal: would .densify() do the job?
# https://github.com/populationgenomics/ancestry/blob/69f05f4d0b20dcd1b0248582989580400717b950/scripts/hail_batch/calculate_ld/calculate_ld.py#L16
# complete the other half of the matrix
bm = bm + bm.T

# TODO: associations missing in LD reference will be dropped here
# TODO: associations_ld_index could be contained insde region_ld_index they could be the same DataFrame
# Index containing the associations of interest
associations_ld_index = (
    associations_ht.key_by("locus37", "alleles")
    .join(ld_index.key_by("locus", "alleles"))
    .add_index("i")
)
# Index with regions around the associations
region_ld_index = (
    ld_index.filter(hl.is_defined(associations_ht.key_by("region37")[ld_index.locus]))
    .add_index("j")
    .persist()
)

# TODO: do we need to use localise?
# Used in ld_matrix function: https://hail.is/docs/0.2/_modules/hail/methods/statgen.html#ld_matrix
start, stop = hl.linalg.utils.locus_windows(
    region_ld_index.locus, ld_window, _localize=False
)
start = start.collect()[0]
stop = stop.collect()[0]

# Indexes for associations and regions
associations_idxs = associations_ld_index.idx.collect()
region_idxs = region_ld_index.idx.collect()
# What region variants are actually associations
associations_idx_inregion = [
    idx for idx, value in enumerate(region_idxs) if value in associations_idxs
]

# TODO: to explore
# How to get pair of variants (for debugging): https://broadinstitute.github.io/gnomad_methods/_modules/gnomad/variant_qc/ld.html#get_r_for_pair_of_variants
# blocks_only (bool) – If False, set all elements outside row intervals to zero. If True, only set all blocks outside row intervals to blocks of zeros; this is more efficient.
# _sparsify_row_intervals_expr is called instead in: https://hail.is/docs/0.2/_modules/hail/methods/statgen.html#ld_matrix
bm_sparse = bm.filter(associations_idxs, region_idxs).sparsify_row_intervals(
    [start[i] for i in associations_idx_inregion],
    [stop[i] for i in associations_idx_inregion],
)

# Get r2 from r
bm_r2 = bm_sparse**2

# Return long format hail table instead of wide matrix
entries = bm_r2.entries()

# Only entries with r2
# TODO: maybe do this earlier in the blockmatrix?
entries = entries.filter(entries.entry >= min_r2)

# Join variant metadata
# TODO: liftover region positions to 38
region_ld_index = region_ld_index.key_by("j")
associations_ld_index = associations_ld_index.key_by("i")
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
        associations_ld_index.select(
            associations_variant=associations_ld_index.locus,
            associations_alleles=associations_ld_index.alleles,
            associations_variant37=associations_ld_index.locus37,
        )
    )
    .key_by("associations_variant")
    .drop("i", "j")
    .rename({"entry": "r2"})
)

# Test case
# https://genetics.opentargets.org/variant/21_13847526_T_C
out = (
    results.to_spark()
    .persist()
    .sort(f.col("r2").desc())
    .filter(f.col("`associations_variant.position`") == "13847526")
)
