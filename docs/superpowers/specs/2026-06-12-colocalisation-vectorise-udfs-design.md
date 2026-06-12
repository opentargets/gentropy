# Vectorise colocalisation posterior UDFs (pandas_udf)

**Date:** 2026-06-12
**Branch:** `perf/colocalisation-vectorise-udfs` (off `dev`)
**Status:** Approved (Approach A)

## Problem

The colocalisation methods compute hypothesis posteriors with **row-at-a-time
Python UDFs** (`f.udf`), which serialise data one row at a time and incur
per-row Python pickling. On a release-scale `coloc_pip_ecaviar` run these UDFs
are CPU-bound and a meaningful (secondary) cost. The dominant cost is the
overlap self-join skew (addressed separately in PR #1232); this work targets
the UDF serialization overhead.

Affected sites:

- `ColocPIP._get_posteriors` (`src/gentropy/method/colocalisation/coloc_pip.py`)
  — one scalar UDF over **ragged per-locus arrays** (variant names + PIPs).
  This is the UDF exercised by the production `coloc_pip_ecaviar` path
  (eCAVIAR itself uses no UDF).
- `Coloc` (`src/gentropy/method/colocalisation/coloc.py`) — a `get_logsum`
  UDF called **3× per row** on array columns, plus `Coloc._get_posteriors`
  on a fixed 5-element array. Only runs for the standalone `coloc` method.

## Goal

Replace the Python UDFs with `pandas_udf` (Arrow-batched), keeping output
**numerically equivalent** to the current implementation. No change to method
signatures, dispatch, schema, or the `coloc_pip_ecaviar` merge.

## Approach A (chosen)

Core technique: replace `f.udf(..., VectorUDT())` with
`f.pandas_udf(..., ArrayType(DoubleType()))` and switch all UDF-facing columns
from `VectorUDT` to `array<double>`. This deletes every `array_to_vector` /
`vector_to_array` wrapper (they exist only to feed vector-typed UDFs); the
existing `.getItem(i)` extraction works unchanged on arrays.

### ColocPIP (`coloc_pip.py`)

- `_get_posteriors` becomes a `pandas_udf` taking the 4 array columns
  (`left_variants`, `left_pips`, `right_variants`, `right_pips`); the 3 priors
  are **bound via closure** (per-job constants) instead of passed as 3 constant
  `f.lit` columns.
- Body iterates the Arrow batch (Series of ragged arrays) applying the **exact
  current per-row math verbatim**; returns `list[float]` `[0,0,0,PP3,PP4]`
  instead of `Vectors.dense(...)`.
- Drop `fml.vector_to_array` at the `h0..h4` extraction.
- Imports: remove `VectorUDT/DenseVector/Vectors/fml`; add `pandas as pd`,
  `ArrayType`.
- Win = Arrow batch serialization replacing the row-at-a-time pickle path.
  (Still loops in Python — ragged variant-name set-union can't go numpy-wide.)

### Coloc (`coloc.py`)

- **logsum**: `f.udf(get_logsum, DoubleType())` → `pandas_udf(DoubleType())`
  mapping `get_logsum` over the batch. Inputs (`left_logBF`, `right_logBF`,
  `sum_log_bf`) become plain `f.collect_list(...)` arrays (drop
  `array_to_vector`). `get_logsum` reused verbatim → bit-identical.
- **`_get_posteriors`** (always exactly 5 BFs): `pandas_udf(ArrayType(DoubleType()))`
  that **stacks the batch into an (n×5) numpy array and vectorises**
  logsumexp→exp across the whole batch. `allBF` input becomes `f.array(...)`
  (drop `array_to_vector`); output drops `vector_to_array`.
- Remove the now-pointless `array_to_vector`→`vector_to_array` round-trip on
  `left/right_posteriorProbability` (never fed to a UDF — only re-read as
  arrays in `anySnpBothSidesHigh`).
- Update the `_get_posteriors` **doctest** to reflect ndarray/list return
  instead of `DenseVector`.

## Correctness strategy (test-first)

1. **Before refactor**: add characterization tests pinning current
   `Coloc.colocalise` and `ColocPIP.colocalise` numeric output (h0–h4 / clpp)
   on a small fixture — golden values captured from current `dev`. Confirm they
   pass on current code, commit.
2. Refactor → the same tests must still pass.
3. Assertions: **exact** for counts / `numberColocalisingVariants`; **tight
   float tolerance (≤1e-12)** for posteriors.

## Numeric-equivalence note

`ColocPIP` and `Coloc` logsum reuse `get_logsum` per row → **bit-identical**.
The only spot that can differ at float-noise level (≤1e-12, from summation
order) is `Coloc._get_posteriors`'s batch-vectorised logsumexp — scientifically
irrelevant, covered by the test tolerance. (Approved default; bit-exact
alternative would loop it per-row, forgoing vectorisation there.)

## Constraints / dependencies

- Requires Arrow (`pyarrow` already a project dependency). Confirm
  `spark.sql.execution.arrow.pyspark.enabled` is effective in the test session.
- No new dependencies.

## Out of scope (YAGNI)

- Native Spark-SQL rewrite of Coloc's logsumexp (Approach C) — possible future
  follow-up if Coloc's logsum stays hot after this change.
- Any change to `ECaviar`, the method dispatch, or the overlap pipeline.

## Files

1. `src/gentropy/method/colocalisation/coloc_pip.py`
2. `src/gentropy/method/colocalisation/coloc.py`
3. `tests/gentropy/method/test_colocalisation_method.py`
