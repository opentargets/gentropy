"""Batch genetic correlation from a study-pair manifest.

Reads a pairs manifest CSV (produced by the representative-study notebook) and
runs LDSC pairwise rg for every pair using pre-munged per-study parquets produced
by :class:`~gentropy.ldsc_munge.LdscMungeStep`.

Architecture
------------
1. Already-computed pairs are filtered out (checkpoint / resume support).
2. The remaining pairs DataFrame is repartitioned so each Spark partition holds
   ``pairs_per_partition`` pairs.
3. ``mapInPandas`` distributes the partitions across cluster executors.
4. Within each executor the UDF reads the two pre-munged study files from GCS
   via ``pyarrow.fs.GcsFileSystem`` (no SparkSession needed inside the UDF).
5. Results are written as a single Spark parquet write (one file per partition).

On Dataproc the executors run with the cluster service account so no explicit
credential configuration is needed inside the UDF.
"""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterator
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.session import Session
from gentropy.method.ldsc import run_ldsc_rg_from_arrays

logger = logging.getLogger(__name__)

# ── Output schema ─────────────────────────────────────────────────────────────

RG_RESULT_SCHEMA = StructType([
    StructField("studyId_1",     StringType(),  False),
    StructField("studyId_2",     StringType(),  False),
    StructField("ancestry",      StringType(),  True),
    StructField("run_status",    StringType(),  True),
    StructField("skip_reason",   StringType(),  True),
    StructField("n_snps",        LongType(),    True),
    StructField("rg",            DoubleType(),  True),
    StructField("rg_se",         DoubleType(),  True),
    StructField("rg_clipped",    BooleanType(), True),
    StructField("h2_1",          DoubleType(),  True),
    StructField("h2_1_se",       DoubleType(),  True),
    StructField("h2_2",          DoubleType(),  True),
    StructField("h2_2_se",       DoubleType(),  True),
    StructField("gcov",          DoubleType(),  True),
    StructField("gcov_se",       DoubleType(),  True),
    StructField("intercept",     DoubleType(),  True),
    StructField("intercept_se",  DoubleType(),  True),
    StructField("M_ldsc",        DoubleType(),  True),
])


PARTITION_SUMMARY_SCHEMA = StructType([
    StructField("n_pairs",          LongType(),   False),
    StructField("n_success",        LongType(),   False),
    StructField("n_skipped",        LongType(),   False),
    StructField("n_error",          LongType(),   False),
    StructField("elapsed_seconds",  DoubleType(), False),
    StructField("output_path",      StringType(), False),
])

# ── Module-level helpers (serialised into each Spark executor) ─────────────────

def _flip_variant_key(key: str) -> str:
    """Swap ref↔alt in a chr_pos_ref_alt variant key.

    Args:
        key (str): Variant key in ``chr_pos_ref_alt`` format.

    Returns:
        str: Key with ref and alt swapped, or the original key if parsing fails.
    """
    parts = key.split("_")
    if len(parts) == 4:
        return f"{parts[0]}_{parts[1]}_{parts[3]}_{parts[2]}"
    return key


def _read_munged(munged_base: str, ancestry: str, study_id: str) -> pd.DataFrame:
    """Read a pre-munged study parquet from GCS via pyarrow.

    Args:
        munged_base (str): GCS base path for pre-munged study parquets.
        ancestry (str): LD ancestry label (e.g. ``"nfe"``).
        study_id (str): Study identifier.

    Returns:
        pd.DataFrame: Columns: variantKey, beta, se, n, L2.
    """
    path = (
        munged_base.rstrip("/")
        + f"/ancestry={ancestry}/studyId={study_id}"
    ).replace("gs://", "")

    gcs = pafs.GcsFileSystem()
    table = pq.read_table(path, filesystem=gcs)
    return table.to_pandas()[["variantKey", "beta", "se", "n", "L2"]]


def _run_rg_pair(
    s1: str,
    s2: str,
    ancestry: str,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    intercept: float | None,
    twostep: float,
    n_blocks: int,
    min_overlap_snps: int,
) -> dict[str, Any]:
    """Run LDSC rg for one pair given pre-loaded DataFrames.

    Args:
        s1 (str): Study ID for trait 1.
        s2 (str): Study ID for trait 2.
        ancestry (str): LD ancestry label.
        df1 (pd.DataFrame): Pre-loaded munged data for study 1.
        df2 (pd.DataFrame): Pre-loaded munged data for study 2.
        intercept (float | None): Fixed cross-trait intercept, or None to estimate.
        twostep (float): LDSC two-step chi-square cut-off.
        n_blocks (int): Number of jackknife blocks.
        min_overlap_snps (int): Minimum overlapping SNPs required to run.

    Returns:
        dict[str, Any]: Result row matching ``RG_RESULT_SCHEMA``.
    """
    base: dict[str, Any] = {
        "studyId_1": s1, "studyId_2": s2, "ancestry": ancestry,
        "run_status": None, "skip_reason": None, "n_snps": None,
        "rg": None, "rg_se": None, "rg_clipped": None,
        "h2_1": None, "h2_1_se": None, "h2_2": None, "h2_2_se": None,
        "gcov": None, "gcov_se": None, "intercept": None, "intercept_se": None,
        "M_ldsc": None,
    }

    # Inner join on variantKey; also try allele-flipped variant keys for trait 2
    merged = df1.merge(df2, on="variantKey", suffixes=("1", "2"))

    df2_flipped = df2.copy()
    df2_flipped["variantKey"] = df2_flipped["variantKey"].apply(_flip_variant_key)
    df2_flipped["beta"] = -df2_flipped["beta"]

    merged_flipped = df1.merge(
        df2_flipped.rename(columns={"beta": "beta2", "se": "se2", "n": "n2", "L2": "L2_2"}),
        on="variantKey",
        suffixes=("", "_f"),
    )

    full = pd.concat([merged, merged_flipped], ignore_index=True).drop_duplicates("variantKey")

    if len(full) < min_overlap_snps:
        return {
            **base,
            "run_status": "skipped",
            "skip_reason": f"only {len(full)} overlapping SNPs (min {min_overlap_snps})",
            "n_snps": len(full),
        }

    beta1 = full["beta1"].values.astype(float)
    se1   = full["se1"].values.astype(float)
    N1    = full["n1"].values.astype(float)
    beta2 = full["beta2"].values.astype(float)
    se2   = full["se2"].values.astype(float)
    N2    = full["n2"].values.astype(float)
    ld    = full["L2"].values.astype(float)

    w_raw = 1.0 / np.maximum(ld, 1.0)
    w_ld  = w_raw / np.mean(w_raw)

    # M_ldsc proxy: geometric mean of the two study LD-score coverages
    m_ldsc = float(np.sqrt(len(df1) * len(df2)))

    try:
        res = run_ldsc_rg_from_arrays(
            beta1=beta1, se1=se1, N1=N1,
            beta2=beta2, se2=se2, N2=N2,
            ld=ld, w_ld=w_ld,
            M_ldsc_scalar=m_ldsc,
            intercept=intercept,
            twostep=twostep,
            n_blocks=n_blocks,
        )
    except Exception as exc:
        return {**base, "run_status": "error", "skip_reason": f"ldsc error: {exc}",
                "n_snps": len(full)}

    intercept_se_val: float | None = None
    if res.get("intercept_se") not in (None, "NA"):
        with contextlib.suppress(TypeError, ValueError):
            intercept_se_val = float(res["intercept_se"])

    return {
        **base,
        "run_status":   "success",
        "n_snps":       int(res["n_snps"]),
        "rg":           res["rg"],
        "rg_se":        res["rg_se"],
        "rg_clipped":   res["rg_clipped"],
        "h2_1":         res["h2_1"],
        "h2_1_se":      res["h2_1_se"],
        "h2_2":         res["h2_2"],
        "h2_2_se":      res["h2_2_se"],
        "gcov":         res["gcov"],
        "gcov_se":      res["gcov_se"],
        "intercept":    float(res["intercept"]) if res.get("intercept") is not None else None,
        "intercept_se": intercept_se_val,
        "M_ldsc":       m_ldsc,
    }


# ── UDF factory (complexity kept low by delegating to module-level helpers) ────

def _make_rg_udf(
    munged_base: str,
    rg_output_path: str,
    twostep: float,
    n_blocks: int,
    intercept: float | None,
    min_overlap_snps: int,
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """Return a ``mapInPandas``-compatible UDF that writes its partition directly to GCS.

    Each partition writes one parquet file to ``rg_output_path`` as soon as it
    completes and yields a single summary row (counts + elapsed time) for logging.

    Args:
        munged_base (str): GCS base path for pre-munged study parquets.
        rg_output_path (str): GCS destination directory for result parquets.
        twostep (float): LDSC two-step chi-square cut-off.
        n_blocks (int): Number of jackknife blocks.
        intercept (float | None): Fixed cross-trait intercept, or None to estimate.
        min_overlap_snps (int): Minimum number of overlapping SNPs required to run.

    Returns:
        Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]: A function
            compatible with ``DataFrame.mapInPandas``.
    """
    def rg_batch_udf(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Process one Spark partition, write results to GCS, and yield a summary row.

        Args:
            batch_iter (Iterator[pd.DataFrame]): Iterator over partition chunks.

        Yields:
            pd.DataFrame: One summary row per partition with counts and elapsed time.
        """
        # Bounded LRU cache shared across all batches in this partition.
        # Because the manifest is sorted by studyId_1, studyId_1 stays hot in the
        # cache; studyId_2 entries are evicted as newer ones arrive.
        # maxsize=3: with 4 cores per executor and ~180MB/study, 4 * 3 * 180MB = 2.2GB
        # peak cache, leaving headroom within the ~6GB Python budget on n1-standard-4.
        lru: OrderedDict[tuple[str, str], pd.DataFrame | Exception] = OrderedDict()
        lru_maxsize = 3

        def get_study(ancestry: str, study_id: str) -> pd.DataFrame | Exception:
            """Return a study DataFrame from the LRU cache, loading from GCS on miss.

            Args:
                ancestry (str): LD ancestry label (e.g. ``"nfe"``).
                study_id (str): Study identifier.

            Returns:
                pd.DataFrame | Exception: Loaded DataFrame or the exception raised on read.
            """
            key = (ancestry, study_id)
            if key in lru:
                lru.move_to_end(key)
                return lru[key]
            try:
                result: pd.DataFrame | Exception = _read_munged(munged_base, ancestry, study_id)
            except Exception as exc:  # noqa: BLE001
                result = exc
            lru[key] = result
            if len(lru) > lru_maxsize:
                lru.popitem(last=False)
            return result

        for batch in batch_iter:
            t0 = time.time()

            rows = []
            for _, row in batch.iterrows():
                s1, s2, anc = row["studyId_1"], row["studyId_2"], row["ancestry"]
                d1 = get_study(anc, s1)  # noqa: B023
                d2 = get_study(anc, s2)  # noqa: B023
                if isinstance(d1, Exception) or isinstance(d2, Exception):
                    err = d1 if isinstance(d1, Exception) else d2
                    rows.append({
                        "studyId_1": s1, "studyId_2": s2, "ancestry": anc,
                        "run_status": "error", "skip_reason": f"read error: {err}",
                        "n_snps": None, "rg": None, "rg_se": None, "rg_clipped": None,
                        "h2_1": None, "h2_1_se": None, "h2_2": None, "h2_2_se": None,
                        "gcov": None, "gcov_se": None, "intercept": None,
                        "intercept_se": None, "M_ldsc": None,
                    })
                else:
                    rows.append(
                        _run_rg_pair(
                            s1, s2, anc, d1, d2,
                            intercept, twostep, n_blocks, min_overlap_snps,
                        )
                    )

            # Write this partition's results directly to GCS without waiting for
            # other partitions — files appear as each executor completes.
            part_gcs = rg_output_path.rstrip("/").replace("gs://", "") + f"/part-{uuid.uuid4()}.parquet"
            pq.write_table(
                pa.Table.from_pandas(pd.DataFrame(rows)),
                part_gcs,
                filesystem=pafs.GcsFileSystem(),
            )
            out_uri = f"gs://{part_gcs}"

            elapsed = time.time() - t0
            n_success = sum(1 for r in rows if r.get("run_status") == "success")
            n_skipped = sum(1 for r in rows if r.get("run_status") == "skipped")
            n_error   = sum(1 for r in rows if r.get("run_status") == "error")

            yield pd.DataFrame([{
                "n_pairs":         len(rows),
                "n_success":       n_success,
                "n_skipped":       n_skipped,
                "n_error":         n_error,
                "elapsed_seconds": elapsed,
                "output_path":     out_uri,
            }])

    return rg_batch_udf


# ── Step class ────────────────────────────────────────────────────────────────

class GeneticCorrelationManifestStep:
    """Run LDSC pairwise rg for all pairs in a manifest using Spark parallelism.

    Pairs that already appear in ``rg_output_path`` (any ``run_status``) are
    skipped automatically, so the step is safe to re-run after partial failures.
    """

    def __init__(
        self,
        session: Session,
        manifest_path: str,
        munged_path: str,
        rg_output_path: str,
        pairs_per_partition: int = 100,
        twostep: float = 30.0,
        n_blocks: int = 200,
        intercept: float | None = None,
        min_overlap_snps: int = 50,
    ) -> None:
        """Initialise and run the manifest genetic correlation step.

        Args:
            session (Session): Gentropy session object.
            manifest_path (str): Path to the pairs manifest CSV.  Required columns:
                ``studyId_1``, ``studyId_2``, ``ancestry``.
            munged_path (str): GCS path to pre-munged study parquets produced by
                :class:`~gentropy.ldsc_munge.LdscMungeStep`.
            rg_output_path (str): Destination parquet path for results.
            pairs_per_partition (int): Number of pairs to process per Spark
                executor task.  Controls parallelism granularity (default 100).
            twostep (float): LDSC two-step chi-square cut-off (default 30).
            n_blocks (int): Number of jackknife blocks (default 200).
            intercept (float | None): Fixed cross-trait intercept, or None to
                estimate.
            min_overlap_snps (int): Skip pairs with fewer overlapping SNPs
                (default 50).
        """
        self.session = session
        self.manifest_path = manifest_path
        self.munged_path = munged_path.rstrip("/")
        self.rg_output_path = rg_output_path
        self.pairs_per_partition = pairs_per_partition
        self.twostep = twostep
        self.n_blocks = n_blocks
        self.intercept = intercept
        self.min_overlap_snps = min_overlap_snps

        self._run()

    def _run(self) -> None:
        """Execute the manifest pipeline."""
        manifest_pd = pd.read_csv(self.manifest_path)[
            ["studyId_1", "studyId_2", "ancestry"]
        ]
        n_total = len(manifest_pd)
        logger.info("Manifest: %d pairs.", n_total)

        done = self._already_computed()
        if done:
            mask = manifest_pd.apply(
                lambda r: (r["studyId_1"], r["studyId_2"]) in done, axis=1
            )
            manifest_pd = manifest_pd[~mask]
            logger.info("  %d pairs already computed — skipping.", n_total - len(manifest_pd))

        if manifest_pd.empty:
            logger.info("All pairs already computed.")
            return

        logger.info("  Running %d pairs …", len(manifest_pd))

        # Sort by studyId_1 so consecutive pairs in each partition share the same
        # studyId_1, maximising LRU cache hits inside the UDF.
        manifest_pd = manifest_pd.sort_values(
            ["studyId_1", "studyId_2"]
        ).reset_index(drop=True)
        n_partitions = max(1, len(manifest_pd) // self.pairs_per_partition)
        manifest_pd["_pid"] = manifest_pd.index // self.pairs_per_partition

        pairs_sdf = self.session.spark.createDataFrame(manifest_pd)
        # Repartition by _pid so sequential groups stay together, then sort within
        # each partition to guarantee studyId_1 ordering before the UDF sees the data.
        pairs_sdf = (
            pairs_sdf.repartition(n_partitions, "_pid")
            .sortWithinPartitions("studyId_1", "studyId_2")
            .drop("_pid")
        )

        udf = _make_rg_udf(
            munged_base=self.munged_path,
            rg_output_path=self.rg_output_path,
            twostep=self.twostep,
            n_blocks=self.n_blocks,
            intercept=self.intercept,
            min_overlap_snps=self.min_overlap_snps,
        )

        summaries = pairs_sdf.mapInPandas(udf, schema=PARTITION_SUMMARY_SCHEMA).collect()
        for s in summaries:
            logger.info(
                "Partition done: %d pairs in %.1fs — success=%d skipped=%d error=%d → %s",
                s.n_pairs, s.elapsed_seconds, s.n_success, s.n_skipped, s.n_error, s.output_path,
            )
        logger.info(
            "All partitions complete: %d pairs total, %d success, %d skipped, %d error",
            sum(s.n_pairs for s in summaries),
            sum(s.n_success for s in summaries),
            sum(s.n_skipped for s in summaries),
            sum(s.n_error for s in summaries),
        )

    def _already_computed(self) -> set[tuple[str, str]]:
        """Return (studyId_1, studyId_2) pairs that already have a result row.

        Returns:
            set[tuple[str, str]]: Set of already-computed pair keys, or empty set
                if the output path does not yet exist.
        """
        try:
            existing = self.session.spark.read.parquet(self.rg_output_path)
            return {
                (row["studyId_1"], row["studyId_2"])
                for row in existing.select("studyId_1", "studyId_2").collect()
            }
        except Exception:
            return set()

    def summary(self) -> DataFrame:
        """Return a Spark DataFrame summarising run_status counts.

        Returns:
            DataFrame: Grouped counts by run_status.
        """
        return (
            self.session.spark.read.parquet(self.rg_output_path)
            .groupBy("run_status")
            .count()
            .orderBy("run_status")
        )
