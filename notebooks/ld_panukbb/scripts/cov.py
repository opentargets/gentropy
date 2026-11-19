# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""
import sys

import polars as pl

d=sys.argv[1]
ld=pl.read_parquet(f"{d}/ldidx22/*.parquet")
e=ld.explode("ldSet").unnest("ldSet")
tagchr=e.with_columns(pl.col("tagVariantId").str.split("_").list.get(0).alias("tc"))
bad=tagchr.filter(pl.col("tc")!="22")
print("chr22 index variants",ld.height,"pairs",e.height,"pairs with tag on other chrom",bad.height, bad.select("variantId").n_unique() if bad.height else 0, bad.head(3))
nfe=e.explode("rValues").unnest("rValues").filter(pl.col("population")=="nfe").select("variantId").unique()
eur=pl.read_parquet(f"{d}/eur/*.parquet").filter(pl.col("chromosome")=="22").select("variantId").unique()
iv=pl.read_parquet("/Users/dc16/data/mendelian_randomisation/inputs_v2_26.06/instrument_variants.parquet/*.parquet").filter(pl.col("variantId").str.starts_with("22_"))
ivs=set(iv["variantId"]); a=ivs&set(ld["variantId"]); b=ivs&set(nfe["variantId"]); c=ivs&set(eur["variantId"])
print(f"chr22 IVs {len(ivs)}: in gnomAD index any pop {len(a)} ({len(a)/len(ivs):.1%}), with nfe r {len(b)} ({len(b)/len(ivs):.1%}), in PanUKBB EUR {len(c)} ({len(c)/len(ivs):.1%}), in EUR but no nfe {len(c-b)}, nfe but not EUR {len(b-c)}")
print("index variants chr22: gnomAD any",ld.height,"gnomAD nfe",nfe.height,"PanUKBB EUR",eur.height)
