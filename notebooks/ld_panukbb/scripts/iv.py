# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""
import polars as pl

d="/Users/dc16/data/mendelian_randomisation/inputs_v2_26.06"
iv=pl.read_parquet(f"{d}/instrument_variants.parquet/*.parquet"); ex=pl.read_parquet(f"{d}/exposure_table.parquet/*.parquet")
print("distinct IV variants",iv.height,"exposure rows",ex.height,"exposures",ex["exposureStudyId"].n_unique())
ex=ex.with_columns(pl.col("variantId").str.split("_").list.get(0).alias("chr"),pl.col("variantId").str.split("_").list.get(1).cast(pl.Int64).alias("pos"))
# pairs of IVs within same exposure+chr within 10Mb: what TwoSampleMR clumping would have to evaluate
p=ex.join(ex,on=["exposureStudyId","chr"],suffix="_b").filter(pl.col("pos")<pl.col("pos_b"))
p=p.with_columns((pl.col("pos_b")-pl.col("pos")).alias("dist"))
for w in (1_000_000,5_000_000,10_000_000):
    q=p.filter(pl.col("dist")<=w); print(f"within {w/1e6:.0f}Mb: same-exposure IV pairs {q.height}, distinct variant pairs {q.select('variantId','variantId_b').unique().height}")
