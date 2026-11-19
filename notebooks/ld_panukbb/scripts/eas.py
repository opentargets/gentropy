# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""

import json

import polars as pl

m = json.load(
    open(
        "/Users/dc16/gentropy/src/gentropy/assets/data/gwas_population_2_LD_panel_map.json"
    )
)
MID = "Greater Middle Eastern (Middle Eastern, North African or Persian)"
true = {
    "East Asian": "EAS",
    "Asian unspecified": "EAS?",
    "South East Asian": "EAS",
    "South Asian": "SAS",
    "Central Asian": "CAS",
    MID: "MID",
    "Oceanian": "OCE",
    "Aboriginal Australian": "OCE",
}
s = pl.read_parquet(
    "/Users/dc16/data/mendelian_randomisation/ot_26.06/study/*.parquet"
).filter(pl.col("projectId") == "GCST")
print("GCST studies", s.height)
d = (
    s.select("studyId", "discoverySamples")
    .explode("discoverySamples")
    .unnest("discoverySamples")
    .filter(pl.col("ancestry").is_not_null())
    .with_columns(
        pl.col("ancestry").replace_strict(m, default=None).alias("mapped"),
        pl.col("ancestry").replace_strict(true, default=None).alias("trueGrp"),
        pl.col("sampleSize").cast(pl.Float64).fill_null(0),
    )
)
print(
    d.filter(pl.col("mapped") == "eas")
    .group_by("ancestry")
    .agg(
        pl.col("studyId").n_unique().alias("studies"),
        pl.col("sampleSize").sum().alias("N"),
    )
    .sort("studies", descending=True)
)
tot = d.group_by("studyId").agg(pl.col("sampleSize").sum().alias("tot"))


def major(col: str) -> pl.DataFrame:
    """Major LD population per study, ties resolved to nfe (as in LDAnnotator).

    Args:
        col (str): Column of `d` holding the mapped LD population label.

    Returns:
        pl.DataFrame: studyId, major.
    """
    a = (
        d.group_by("studyId", col)
        .agg(pl.col("sampleSize").sum().alias("n"))
        .join(tot, on="studyId")
        .with_columns((pl.col("n") / pl.col("tot")).alias("share"))
    )
    mx = a.group_by("studyId").agg(pl.col("share").max().alias("mx"))
    t = a.join(mx, on="studyId").filter(pl.col("share") == pl.col("mx"))
    return t.group_by("studyId").agg(
        pl.when((pl.col(col) == "nfe").any() & (pl.len() > 1))
        .then(pl.lit("nfe"))
        .otherwise(pl.col(col).first())
        .alias("major")
    )


cur = major("mapped")
print(
    "current major pop (GCST):",
    cur.group_by("major").len().sort("len", descending=True),
)
e = (
    d.filter(pl.col("mapped") == "eas")
    .group_by("studyId", "trueGrp")
    .agg(pl.col("sampleSize").sum().alias("n"))
)
dom = e.sort("n", descending=True).group_by("studyId", maintain_order=True).first()
em = cur.filter(pl.col("major") == "eas").join(dom, on="studyId", how="left")
print(
    "eas-major studies by dominant true group in the eas bucket:",
    em.group_by("trueGrp").len().sort("len", descending=True),
)
m2 = dict(m) | {"South Asian": "csa", "Central Asian": "csa", MID: "mid"}
d = d.with_columns(pl.col("ancestry").replace_strict(m2, default=None).alias("mapped2"))
new = major("mapped2")
ch = cur.join(new, on="studyId", suffix="_new").filter(
    pl.col("major") != pl.col("major_new")
)
print("studies whose major pop changes (SAS/CAS->csa, MID->mid):", ch.height)
print(ch.group_by("major", "major_new").len().sort("len", descending=True))
ex = (
    pl.read_parquet(
        "/Users/dc16/data/mendelian_randomisation/inputs_v2_26.06/exposure_table.parquet/*.parquet"
    )
    .select("exposureStudyId")
    .unique()
)
print(
    "MR exposures among changed:",
    ch.join(ex, left_on="studyId", right_on="exposureStudyId").height,
    "| MR exposures eas-major:",
    cur.filter(pl.col("major") == "eas")
    .join(ex, left_on="studyId", right_on="exposureStudyId")
    .height,
)
