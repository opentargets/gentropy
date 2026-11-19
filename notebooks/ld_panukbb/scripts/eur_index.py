# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""
import sys

import polars as pl

d=sys.argv[1]
df=pl.read_parquet(f"{d}/eur/*.parquet")
print(df.schema); print(df.height, "rows;", df["idx"].n_unique(), "distinct idx")
print(df.group_by("alleleOrder").len())
df=df.sort("idx")
# bp span of 21 and 37 blocks of 4096 variants
bs=4096
for nblk in (21,37):
    w=nblk*bs
    s=df.select(pl.col("chromosome"),pl.col("position"),pl.col("idx"))
    s=s.with_columns((pl.col("position").shift(-w)-pl.col("position")).alias("span"),(pl.col("chromosome").shift(-w)==pl.col("chromosome")).alias("same"))
    s=s.filter(pl.col("same"))
    print(nblk,"blocks ->",w,"variants; span bp median",s["span"].median(),"p10",s["span"].quantile(0.1),"p90",s["span"].quantile(0.9))
print("per-chrom", df.group_by("chromosome").len().sort("chromosome").head(30))
