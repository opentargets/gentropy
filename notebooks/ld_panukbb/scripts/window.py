# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""
import polars as pl

d="/Users/dc16/data/mendelian_randomisation/inputs_v2_26.06"
ex=pl.read_parquet(f"{d}/exposure_table.parquet/*.parquet")
print(ex.columns)
ex=ex.with_columns(pl.col("variantId").str.split("_").list.get(0).alias("chr"),pl.col("variantId").str.split("_").list.get(1).cast(pl.Int64).alias("pos"),
  (pl.col("exposureBeta")/pl.col("exposureStandardError")).abs().alias("z")).sort(["exposureStudyId","chr","z"],descending=[False,False,True])
def clump(g: pl.DataFrame, w: int) -> int:
    """Greedy window clumping of one exposure/chromosome group, rows pre-sorted by |z| desc.

    Args:
        g (pl.DataFrame): IVs of one exposure on one chromosome, with a `pos` column.
        w (int): Window in bp.

    Returns:
        int: Number of IVs kept.
    """
    pos=g["pos"].to_numpy(); keep: list[int]=[]
    for i,p in enumerate(pos):
        if all(abs(p-pos[k])>w for k in keep): keep.append(i)
    return len(keep)
tot=ex.height
for w in (500_000,1_000_000,10_000_000):
    kept=sum(clump(g,w) for _,g in ex.group_by(["exposureStudyId","chr"]))
    print(f"window {w/1e6:g} Mb: keep {kept}/{tot} IV rows ({kept/tot:.1%})")
per=ex.group_by("exposureStudyId").len()
print("exposures",per.height,"median IVs",per["len"].median())
