# Colocalisation Posterior Optimisation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the Python-UDF overhead from colocalisation posterior computation — Coloc becomes native Spark SQL, ColocPIP becomes an Arrow-batched `pandas_udf` — with numerically equivalent output.

**Architecture:** Add a native `get_logsum_column` (logsumexp as a Spark column expression) to `common/stats.py`. Rewrite `Coloc.colocalise` to use it for `logsum1/2/12` and to compute the 5-hypothesis softmax natively, deleting `Coloc._get_posteriors`. Convert `ColocPIP._get_posteriors` to a `pandas_udf` (priors bound via closure) returning `array<double>`. All UDF-facing columns switch from `VectorUDT` to `array<double>`, removing every `array_to_vector`/`vector_to_array` wrapper.

**Tech Stack:** PySpark (SQL functions + higher-order functions `transform`/`aggregate`, `pandas_udf`), NumPy, pytest, uv.

**Test command (used throughout):**

```
uv run --group test --group dev pytest <targets> -q
```

**Spec:** `docs/superpowers/specs/2026-06-12-colocalisation-vectorise-udfs-design.md`

---

### Task 1: Native `get_logsum_column` helper in `common/stats.py`

**Files:**

- Modify: `src/gentropy/common/stats.py` (add function after `get_logsum`, ~line 42)
- Test: doctest in the function + `tests/gentropy/common/test_stats.py` (create if absent)

`stats.py` already imports `from pyspark.sql import Column` and `from pyspark.sql import functions as f` — no new imports needed.

- [ ] **Step 1: Write the failing unit test**

Create/append `tests/gentropy/common/test_stats.py`:

```python
"""Tests for gentropy.common.stats."""

from __future__ import annotations

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.common.stats import get_logsum, get_logsum_column


def test_get_logsum_column_matches_get_logsum(spark: SparkSession) -> None:
    """Native column logsumexp must match the numpy get_logsum."""
    arrays = [[0.2, 0.1, 0.05, 0.0], [10.3, 10.5], [1.2, 3.8, 10.2], [-5.0]]
    df = spark.createDataFrame([(i, a) for i, a in enumerate(arrays)], ["id", "arr"])
    rows = df.select("id", get_logsum_column(f.col("arr")).alias("ls")).collect()
    observed = {r["id"]: r["ls"] for r in rows}
    for i, a in enumerate(arrays):
        expected = get_logsum(np.array(a, dtype=np.float64))
        assert np.isclose(observed[i], expected, atol=1e-12), (
            f"mismatch for {a}: {observed[i]} vs {expected}"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --group test --group dev pytest tests/gentropy/common/test_stats.py -q`
Expected: FAIL with `ImportError: cannot import name 'get_logsum_column'`.

- [ ] **Step 3: Implement `get_logsum_column`**

Add to `src/gentropy/common/stats.py` immediately after `get_logsum` (after line 42):

```python
def get_logsum_column(arr: Column) -> Column:
    """Native Spark column logsumexp, matching :func:`get_logsum`.

    Computes ``max(arr) + log(sum(exp(arr - max(arr))))`` using only Spark SQL
    expressions, so it runs without Python serialization.

    Args:
        arr (Column): array<double> column.

    Returns:
        Column: logsumexp of the array, as a double column.

    Examples:
        >>> df = spark.createDataFrame([([0.2, 0.1, 0.05, 0.0],)], ["arr"])
        >>> df.select(f.round(get_logsum_column(f.col("arr")), 6).alias("ls")).show()
        +--------+
        |      ls|
        +--------+
        |1.476557|
        +--------+
        <BLANKLINE>
    """
    max_val = f.array_max(arr)
    return max_val + f.log(
        f.aggregate(
            f.transform(arr, lambda x: f.exp(x - max_val)),
            f.lit(0.0),
            lambda acc, x: acc + x,
        )
    )
```

- [ ] **Step 4: Run tests (unit + doctest) to verify they pass**

Run: `uv run --group test --group dev pytest tests/gentropy/common/test_stats.py "src/gentropy/common/stats.py" -q`
Expected: PASS (unit test + the new doctest). If the doctest errors with `name 'spark' is not defined`, the doctest namespace lacks a Spark session — in that case delete the doctest's `>>>` block and rely on the unit test (the unit test is the authority).

- [ ] **Step 5: Commit**

```bash
git add src/gentropy/common/stats.py tests/gentropy/common/test_stats.py
git commit -m "feat(stats): add native get_logsum_column (Spark-SQL logsumexp)"
```

---

### Task 2: Characterization tests locking current posterior output

**Files:**

- Test: `tests/gentropy/method/test_colocalisation_method.py` (append two tests)

These pin the CURRENT numeric output at ≤1e-9 so the refactor is provably equivalent. Golden values are the ones already documented in `test_coloc_semantic` (case 1).

- [ ] **Step 1: Write the characterization tests**

Append to `tests/gentropy/method/test_colocalisation_method.py`:

```python
def test_coloc_characterization(spark: SparkSession) -> None:
    """Pin Coloc posteriors at 1e-9 (single high-PP overlapping SNP)."""
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            cast(
                Any,
                [
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp",
                        "statistics": {
                            "left_logBF": 10.3,
                            "right_logBF": 10.5,
                            "left_beta": 0.1,
                            "right_beta": 0.2,
                            "left_posteriorProbability": 0.91,
                            "right_posteriorProbability": 0.92,
                        },
                    }
                ],
            ),
            schema=StudyLocusOverlap.get_schema(),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )
    row = (
        Coloc.colocalise(observed_overlap, overlap_size_cutoff=5, posterior_cutoff=0.1)
        .df.select("h0", "h1", "h2", "h3", "h4")
        .collect()[0]
        .asDict()
    )
    expected = {
        "h0": 9.254841951638903e-5,
        "h1": 2.7517068829182966e-4,
        "h2": 3.3609423764447284e-4,
        "h3": 9.254841952564387e-13,
        "h4": 0.9992961866536217,
    }
    for k, v in expected.items():
        assert abs(row[k] - v) <= 1e-9, f"{k}: {row[k]} != {v}"


def test_coloc_pip_characterization(spark: SparkSession) -> None:
    """Pin ColocPIP h3/h4 at 1e-9 (single high-PIP overlapping SNP)."""
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            cast(
                Any,
                [
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp1",
                        "statistics": {
                            "left_posteriorProbability": 0.95,
                            "right_posteriorProbability": 0.90,
                            "left_beta": 0.5,
                            "right_beta": 0.3,
                        },
                    }
                ],
            ),
            schema=StudyLocusOverlap.get_schema(),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )
    row = ColocPIP.colocalise(observed_overlap).df.collect()[0].asDict()
    # H0-H2 are exactly zero in ColocPIP; H3+H4 normalise to 1.
    assert row["h0"] == 0.0 and row["h1"] == 0.0 and row["h2"] == 0.0
    # Single shared SNP is the degenerate diff_arg==0 branch: h3 -> 0, h4 -> 1
    # in closed form. This pins the normalisation and the degenerate path.
    assert abs(row["h3"] - 0.0) <= 1e-9, f"h3: {row['h3']}"
    assert abs(row["h4"] - 1.0) <= 1e-9, f"h4: {row['h4']}"
```

- [ ] **Step 2: Run both characterization tests on current code to verify they pass**

Run: `uv run --group test --group dev pytest tests/gentropy/method/test_colocalisation_method.py -k characterization -q`
Expected: PASS (this locks current behaviour as the baseline).

- [ ] **Step 3: Commit**

```bash
git add tests/gentropy/method/test_colocalisation_method.py
git commit -m "test(coloc): characterization tests pinning posterior output at 1e-9"
```

---

### Task 3: Convert `ColocPIP._get_posteriors` to a pandas_udf

**Files:**

- Modify: `src/gentropy/method/colocalisation/coloc_pip.py`
- Test: `tests/gentropy/method/test_colocalisation_method.py` (existing + Task 2 tests)

- [ ] **Step 1: Update imports**

In `coloc_pip.py`, change the import block. Remove:

```python
import pyspark.ml.functions as fml
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
```

Add:

```python
import pandas as pd
```

Keep `import numpy as np`, `from pyspark.sql import functions as f`, `from pyspark.sql import types as t`, `from gentropy.common.stats import get_logsum`. (`DenseVector` is still referenced in the `_get_posteriors` return type hint — update that hint in Step 3.)

- [ ] **Step 2: Replace the UDF registration + application**

In `ColocPIP.colocalise`, replace:

```python
        # Register UDF for calculating posteriors from PIPs
        posteriors_udf = f.udf(cls._get_posteriors, VectorUDT())
```

with:

```python
        priorc1, priorc2, priorc12 = config.priorc1, config.priorc2, config.priorc12

        def _posteriors_batch(
            left_variants: pd.Series,
            left_pips: pd.Series,
            right_variants: pd.Series,
            right_pips: pd.Series,
        ) -> pd.Series:
            return pd.Series(
                [
                    cls._get_posteriors(lv, lp, rv, rp, priorc1, priorc2, priorc12)
                    for lv, lp, rv, rp in zip(
                        left_variants, left_pips, right_variants, right_pips
                    )
                ]
            )

        posteriors_udf = f.pandas_udf(_posteriors_batch, t.ArrayType(t.DoubleType()))
```

Then replace the posterior extraction block:

```python
                # Extract individual hypothesis posteriors
                .withColumn("h0", fml.vector_to_array(f.col("posteriors")).getItem(0))
                .withColumn("h1", fml.vector_to_array(f.col("posteriors")).getItem(1))
                .withColumn("h2", fml.vector_to_array(f.col("posteriors")).getItem(2))
                .withColumn("h3", fml.vector_to_array(f.col("posteriors")).getItem(3))
                .withColumn("h4", fml.vector_to_array(f.col("posteriors")).getItem(4))
```

with:

```python
                # Extract individual hypothesis posteriors
                .withColumn("h0", f.col("posteriors").getItem(0))
                .withColumn("h1", f.col("posteriors").getItem(1))
                .withColumn("h2", f.col("posteriors").getItem(2))
                .withColumn("h3", f.col("posteriors").getItem(3))
                .withColumn("h4", f.col("posteriors").getItem(4))
```

The `posteriors_udf(...)` call itself (the `.withColumn("posteriors", posteriors_udf(...))` with the 4 cast columns) is unchanged EXCEPT remove the 3 trailing `f.lit(...)` prior arguments — the udf now takes only the 4 array columns:

```python
                .withColumn(
                    "posteriors",
                    posteriors_udf(
                        f.col("left_variants").cast("array<string>"),
                        f.col("left_pips").cast("array<double>"),
                        f.col("right_variants").cast("array<string>"),
                        f.col("right_pips").cast("array<double>"),
                    ),
                )
```

- [ ] **Step 3: Change `_get_posteriors` return type to a plain list**

In `ColocPIP._get_posteriors`, change the signature return hint and final return. Replace:

```python
    ) -> DenseVector:
```

with:

```python
    ) -> list[float]:
```

and replace:

```python
        return Vectors.dense([0.0, 0.0, 0.0, PP3, PP4])
```

with:

```python
        return [0.0, 0.0, 0.0, float(PP3), float(PP4)]
```

Update the docstring `Returns:` line from `DenseVector: ...` to `list[float]: [H0, H1, H2, H3, H4] posteriors`.

- [ ] **Step 4: Run ColocPIP tests to verify they pass**

Run: `uv run --group test --group dev pytest tests/gentropy/method/test_colocalisation_method.py -k "coloc_pip" -q`
Expected: PASS (including `test_coloc_pip_characterization`, `test_coloc_pip_semantic`, `test_coloc_pip_priors`, `test_coloc_pip_null_pips`, `test_coloc_pip_type_error`, `test_coloc_pip_beta_ratio`).

- [ ] **Step 5: Commit**

```bash
git add src/gentropy/method/colocalisation/coloc_pip.py
git commit -m "perf(coloc): vectorise ColocPIP posteriors with pandas_udf"
```

---

### Task 4: Rewrite `Coloc` to native Spark SQL (no UDFs)

**Files:**

- Modify: `src/gentropy/method/colocalisation/coloc.py`
- Test: `tests/gentropy/method/test_colocalisation_method.py`

- [ ] **Step 1: Update imports**

In `coloc.py`, remove:

```python
import numpy as np
import pyspark.ml.functions as fml
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
from pyspark.sql.types import DoubleType
```

and change:

```python
from gentropy.common.stats import get_logsum
```

to:

```python
from gentropy.common.stats import get_logsum_column
```

Keep `import pyspark.sql.functions as f` and `import pyspark.sql.types as t`. Also remove the now-unused `from numpy.typing import NDArray` under `TYPE_CHECKING` (it was only used by `_get_posteriors`).

- [ ] **Step 2: Remove the UDF registrations**

Delete these lines from `Coloc.colocalise`:

```python
        # register udfs
        logsum = f.udf(get_logsum, DoubleType())
        posteriors = f.udf(Coloc._get_posteriors, VectorUDT())
```

- [ ] **Step 3: Collect arrays instead of vectors in the aggregation**

In the `.agg(...)`, replace each `fml.array_to_vector(f.collect_list(...))` with plain `f.collect_list(...)`. Specifically the five aggregates become:

```python
                    f.collect_list(f.col("left_logBF")).alias("left_logBF"),
                    f.collect_list(f.col("right_logBF")).alias("right_logBF"),
                    f.collect_list(f.col("left_posteriorProbability")).alias(
                        "left_posteriorProbability"
                    ),
                    f.collect_list(f.col("right_posteriorProbability")).alias(
                        "right_posteriorProbability"
                    ),
                    f.collect_list(f.col("sum_log_bf")).alias("sum_log_bf"),
```

(The `numberColocalisingVariants` and `tagVariantSourceList` aggregates are unchanged.)

- [ ] **Step 4: Use native logsumexp for logsum1/2/12**

Replace:

```python
                .withColumn("logsum1", logsum(f.col("left_logBF")))
                .withColumn("logsum2", logsum(f.col("right_logBF")))
                .withColumn("logsum12", logsum(f.col("sum_log_bf")))
```

with:

```python
                .withColumn("logsum1", get_logsum_column(f.col("left_logBF")))
                .withColumn("logsum2", get_logsum_column(f.col("right_logBF")))
                .withColumn("logsum12", get_logsum_column(f.col("sum_log_bf")))
```

- [ ] **Step 5: Drop vector_to_array in `anySnpBothSidesHigh`**

In the `f.arrays_zip(...)` inside the `anySnpBothSidesHigh` column, replace:

```python
                                fml.vector_to_array(f.col("left_posteriorProbability")),
                                fml.vector_to_array(
                                    f.col("right_posteriorProbability")
                                ),
```

with:

```python
                                f.col("left_posteriorProbability"),
                                f.col("right_posteriorProbability"),
```

- [ ] **Step 6: Replace the posteriors UDF block with native softmax**

Replace this block (the `allBF` build through the `h0..h4` extraction):

```python
                # posteriors
                .withColumn(
                    "allBF",
                    fml.array_to_vector(
                        f.array(
                            f.col("lH0bf"),
                            f.col("lH1bf"),
                            f.col("lH2bf"),
                            f.col("lH3bf"),
                            f.col("lH4bf"),
                        )
                    ),
                )
                .withColumn(
                    "posteriors", fml.vector_to_array(posteriors(f.col("allBF")))
                )
                .withColumn("h0", f.col("posteriors").getItem(0))
                .withColumn("h1", f.col("posteriors").getItem(1))
                .withColumn("h2", f.col("posteriors").getItem(2))
                .withColumn("h3", f.col("posteriors").getItem(3))
                .withColumn("h4", f.col("posteriors").getItem(4))
                # clean up
                .drop(
                    "posteriors",
                    "allBF",
                    "lH0bf",
                    "lH1bf",
                    "lH2bf",
                    "lH3bf",
                    "lH4bf",
                    "left_posteriorProbability",
                    "right_posteriorProbability",
                    "tagVariantSourceList",
                    "anySnpBothSidesHigh",
                )
```

with:

```python
                # posteriors: softmax over the 5 hypothesis Bayes factors,
                # computed natively as exp(lH_i - logsumexp(allBF)).
                .withColumn(
                    "coloc_log_denom",
                    get_logsum_column(
                        f.array(
                            f.col("lH0bf"),
                            f.col("lH1bf"),
                            f.col("lH2bf"),
                            f.col("lH3bf"),
                            f.col("lH4bf"),
                        )
                    ),
                )
                .withColumn("h0", f.exp(f.col("lH0bf") - f.col("coloc_log_denom")))
                .withColumn("h1", f.exp(f.col("lH1bf") - f.col("coloc_log_denom")))
                .withColumn("h2", f.exp(f.col("lH2bf") - f.col("coloc_log_denom")))
                .withColumn("h3", f.exp(f.col("lH3bf") - f.col("coloc_log_denom")))
                .withColumn("h4", f.exp(f.col("lH4bf") - f.col("coloc_log_denom")))
                # clean up
                .drop(
                    "coloc_log_denom",
                    "lH0bf",
                    "lH1bf",
                    "lH2bf",
                    "lH3bf",
                    "lH4bf",
                    "left_posteriorProbability",
                    "right_posteriorProbability",
                    "tagVariantSourceList",
                    "anySnpBothSidesHigh",
                )
```

- [ ] **Step 7: Delete the `_get_posteriors` static method**

Remove the entire `Coloc._get_posteriors` method (from `@staticmethod` through `return Vectors.dense(bfs_posteriors)`), as it is now unused.

- [ ] **Step 8: Run Coloc tests to verify they pass**

Run: `uv run --group test --group dev pytest tests/gentropy/method/test_colocalisation_method.py -k "coloc and not pip" -q`
Expected: PASS (including `test_coloc`, `test_coloc_semantic` (all 5 cases), `test_coloc_characterization`, `test_coloc_no_logbf`, `test_coloc_no_betas`).

- [ ] **Step 9: Commit**

```bash
git add src/gentropy/method/colocalisation/coloc.py
git commit -m "perf(coloc): rewrite Coloc posteriors in native Spark SQL"
```

---

### Task 5: Full verification (methods, step, doctests, lint)

**Files:** none (verification only)

- [ ] **Step 1: Run the full colocalisation test surface**

Run:

```
uv run --group test --group dev pytest tests/gentropy/method/test_colocalisation_method.py tests/gentropy/step/test_colocalisation_step.py tests/gentropy/dataset/test_colocalisation.py tests/gentropy/common/test_stats.py "src/gentropy/method/colocalisation/coloc.py" "src/gentropy/method/colocalisation/coloc_pip.py" "src/gentropy/common/stats.py" -q
```

Expected: PASS (unit, semantic, characterization, step, and doctests).

- [ ] **Step 2: Confirm no stray vector/UDF references remain**

Run: `grep -nE "array_to_vector|vector_to_array|VectorUDT|f\.udf\(" src/gentropy/method/colocalisation/coloc.py src/gentropy/method/colocalisation/coloc_pip.py`
Expected: no output (Coloc fully native; ColocPIP only uses `pandas_udf`).

- [ ] **Step 3: Lint the changed files**

Run: `uv run ruff check src/gentropy/common/stats.py src/gentropy/method/colocalisation/coloc.py src/gentropy/method/colocalisation/coloc_pip.py`
Expected: no errors (catches unused imports left behind). Fix any unused-import findings and re-run.

- [ ] **Step 4: Final commit (only if Step 3 required fixes)**

```bash
git add -A
git commit -m "chore(coloc): drop unused imports after UDF removal"
```

---

## Notes for the implementer

- `pandas_udf` requires `pyarrow` (already a project dependency); it does not depend on `spark.sql.execution.arrow.pyspark.enabled`.
- Numeric equivalence: ColocPIP stays bit-identical (same per-row math via `get_logsum`); Coloc is equivalent within ≤1e-12 (Spark sequential `aggregate` sum vs numpy summation differ only for loci with >128 tags), covered by the 1e-9 characterization tolerance.
- Do NOT touch `find_overlaps`, `ECaviar`, the method dispatch, or `coloc_pip_ecaviar` — those are out of scope (the overlap changes live in separate PR #1232).
