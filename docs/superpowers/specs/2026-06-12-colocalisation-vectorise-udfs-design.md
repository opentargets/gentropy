# Optimise colocalisation posterior computation (Coloc native-SQL + ColocPIP pandas_udf)

**Date:** 2026-06-12
**Branch:** `perf/colocalisation-vectorise-udfs` (off `dev`)
**Status:** Approved (Approach A+C)

## Problem

The colocalisation methods compute hypothesis posteriors with **row-at-a-time
Python UDFs** (`f.udf`), which serialise data one row at a time and incur
per-row Python pickling. On a release-scale `coloc_pip_ecaviar` run these UDFs
are CPU-bound and a meaningful (secondary) cost. The dominant cost is the
overlap self-join skew (addressed by the `study_locus.py`/`colocalisation.py`
changes that ship alongside this work in the same PR, #1232); this work targets
the UDF serialization/compute overhead.

Affected sites:

- `ColocPIP._get_posteriors` (`src/gentropy/method/colocalisation/coloc_pip.py`)
  — one scalar UDF over **ragged per-locus arrays** (variant names + PIPs).
  This is the UDF exercised by the production `coloc_pip_ecaviar` path
  (eCAVIAR itself uses no UDF).
- `Coloc` (`src/gentropy/method/colocalisation/coloc.py`) — a `get_logsum`
  UDF called **3× per row** on array columns, plus `Coloc._get_posteriors`
  on a fixed 5-element array. Only runs for the standalone `coloc` method.

## Goal

Eliminate the Python-UDF overhead, keeping output **numerically equivalent** to
the current implementation. No change to method signatures, dispatch, schema, or
the `coloc_pip_ecaviar` merge.

## Chosen approach (A + C)

Two different techniques, picked per method by what the data shape allows:

- **ColocPIP → `pandas_udf` (Approach A).** Its ragged variant-name set-union
  across two PIP arrays does not express cleanly in Spark SQL, so it stays a
  UDF — but Arrow-batched instead of row-at-a-time.
- **Coloc → native Spark SQL (Approach C).** Both its UDFs (logsumexp and the
  5-hypothesis softmax) are pure log-space arithmetic that maps directly onto
  native `array_max`/`transform`/`aggregate`/`exp`/`log`, removing Python
  entirely.

### ColocPIP (`coloc_pip.py`)

- `_get_posteriors` becomes a `pandas_udf` taking the 4 array columns
  (`left_variants`, `left_pips`, `right_variants`, `right_pips`); the 3 priors
  are **bound via closure** (per-job constants) instead of passed as 3 constant
  `f.lit` columns.
- Body iterates the Arrow batch (Series of ragged arrays) applying the **exact
  current per-row math verbatim** (still calls `get_logsum` per row); returns
  `list[float]` `[0,0,0,PP3,PP4]` instead of `Vectors.dense(...)`. Return type
  `ArrayType(DoubleType())`.
- Drop `fml.vector_to_array` at the `h0..h4` extraction (the column is now an
  array).
- Imports: remove `VectorUDT/DenseVector/Vectors/fml`; add `pandas as pd`,
  `ArrayType`.
- Win = Arrow batch serialization replacing the row-at-a-time pickle path.
  (Still loops in Python — ragged set-union can't go numpy-wide.)
- **Numerically bit-identical** to current code.

### Shared helper (`common/stats.py`)

Add a native column-expression sibling to `get_logsum`:

```python
def get_logsum_column(arr: Column) -> Column:
    m = f.array_max(arr)
    return m + f.log(
        f.aggregate(
            f.transform(arr, lambda x: f.exp(x - m)),
            f.lit(0.0),
            lambda acc, x: acc + x,
        )
    )
```

With a doctest pinned to the same value as `get_logsum`
(`[0.2, 0.1, 0.05, 0] → 1.476557`).

### Coloc (`coloc.py`) — fully UDF-free

- **logsum1/2/12**: replace the 3 `logsum(...)` UDF calls with
  `get_logsum_column(...)` on plain `f.collect_list(...)` arrays (drop the
  `array_to_vector` wrappers).
- **posteriors (softmax)**: replace the `_get_posteriors` UDF with native
  columns — `denom = get_logsum_column(f.array(lH0bf..lH4bf))`, then
  `h_i = f.exp(f.col("lH{i}bf") - denom)`. Delete the `_get_posteriors` method
  (and its doctest), the `allBF` build, and the `array_to_vector` /
  `vector_to_array` wrappers.
- Remove the dead `array_to_vector`→`vector_to_array` round-trip on
  `left/right_posteriorProbability` (never fed to a UDF — only re-read as
  arrays in `anySnpBothSidesHigh`).
- The existing `logdiff`/`max`/h3 native log-space block is untouched.
- Drop now-unused imports (`fml`, `VectorUDT/DenseVector/Vectors`, `DoubleType`,
  `np`, `get_logsum`).
- Result: **zero Python/serialization in Coloc**.

## Correctness strategy (test-first)

1. **Before refactor**: add characterization tests pinning current
   `Coloc.colocalise` and `ColocPIP.colocalise` numeric output (h0–h4 / clpp)
   on a small fixture — golden values captured from current `dev`. Confirm they
   pass on current code, commit.
2. Refactor → the same tests must still pass.
3. Assertions: **exact** for counts / `numberColocalisingVariants`; **≤1e-9 abs
   tolerance** for all posteriors.

## Numeric-equivalence note

- **ColocPIP**: **bit-identical** (`get_logsum` per row, unchanged math).
- **Coloc**: **float-tolerance equivalent throughout**, not bit-identical.
  Spark's sequential `aggregate` sum and numpy's summation differ in float
  ordering for **large loci (>128 tags)**; for ≤128 elements they match, so the
  5-element softmax is effectively exact. Differences are ≤1e-12 relative,
  scientifically irrelevant — covered by the ≤1e-9 test tolerance. (Approved
  trade-off: bit-exactness on Coloc given up to eliminate its Python.)

## Risk

Higher than a pure pandas_udf change: Coloc's `logsum1/2/12` and softmax are
rewritten in SQL. Mitigated by (a) the existing `logdiff` block already being
native log-space math, (b) `get_logsum_column` carrying its own doctest pinned
to `get_logsum`'s value, and (c) characterization tests on full
`Coloc.colocalise` output.

## Constraints / dependencies

- ColocPIP `pandas_udf` requires Arrow (`pyarrow` already a project
  dependency). Confirm `spark.sql.execution.arrow.pyspark.enabled` is effective
  in the test session.
- No new dependencies.

## Out of scope (YAGNI)

- Any change to `ECaviar` or the method dispatch.
- The overlap-pipeline changes (`StudyLocus._overlapping_peaks` repartition,
  `ColocalisationStep` persist) are out of scope *for this design doc* but ship
  in the same PR (#1232) as a separate, complementary set of commits.

## Files

1. `src/gentropy/common/stats.py` (new `get_logsum_column` helper)
2. `src/gentropy/method/colocalisation/coloc_pip.py`
3. `src/gentropy/method/colocalisation/coloc.py`
4. `tests/gentropy/method/test_colocalisation_method.py`
