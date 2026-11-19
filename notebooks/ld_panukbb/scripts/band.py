# ruff: noqa: D103, E701, E702, T201
"""Throwaway profiling script for ../PLAN.md (see the Scripts section there)."""
import collections
import json

for P in ["EUR","AFR","CSA"]:
    m=json.load(open(f"meta_{P}.json"))
    bs=m["blockSize"]; n=m["nRows"]; nb=-(-n//bs); kept=m["maybeFiltered"]
    # hail block index = i + j*nRowBlocks (column-major)
    rows=collections.defaultdict(list)
    for k in kept: rows[k%nb].append(k//nb)
    widths=[max(abs(c-r) for c in cs) for r,cs in rows.items()]
    lower=sum(1 for r,cs in rows.items() for c in cs if c<r); upper=sum(1 for r,cs in rows.items() for c in cs if c>r)
    print(P,"n=",n,"blocks/side",nb,"kept",len(kept),"of",nb*nb,f"({len(kept)/nb/nb:.4%})","max band (blocks)",max(widths),"median",sorted(widths)[len(widths)//2],"lower",lower,"upper",upper)
