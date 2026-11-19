# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""
import sys

import polars as pl

df=pl.read_parquet(sys.argv[1]+"/*.parquet")
print(df.schema); print("variants in 100/1000 parts:",df.height)
x=df.select("variantId",pl.col("ldSet").list.len().alias("n"))
print("ldSet size: mean",x["n"].mean(),"median",x["n"].median(),"p99",x["n"].quantile(.99))
e=df.select("variantId",pl.col("ldSet")).explode("ldSet").unnest("ldSet").explode("rValues").unnest("rValues")
print("(variant,tag,pop) rows",e.height); print(e.group_by("population").agg(pl.len(),pl.col("variantId").n_unique().alias("variants")).sort("population"))
e=e.with_columns(pl.col("variantId").str.split("_").list.get(1).cast(pl.Int64).alias("p"),pl.col("tagVariantId").str.split("_").list.get(1).cast(pl.Int64).alias("q"))
d=(pl.col("p")-pl.col("q")).abs()
print("pair distance bp: max",e.select(d.max()).item(),"p99",e.select(d.quantile(.99)).item())
