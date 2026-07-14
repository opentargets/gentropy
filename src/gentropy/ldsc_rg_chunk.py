"""Batch chunk step: process a CSV of study pairs, reading sumstats directly.

Designed to run as a lightweight Google Batch task.  Reads raw harmonised
summary statistics and LD scores from GCS via pyarrow — no Dataproc or
pre-munging required — then runs LDSC pairwise rg for each pair and writes
the results as parquet.

Performance notes
-----------------
LD scores are loaded once per ancestry and cached in memory for the lifetime
of the task.  Sumstats are loaded once per unique study path and cached.
With a small chunk_size (default 5), task runtime stays similar to a
single ``GeneticCorrelationStep`` run while avoiding the per-pair Spark
session startup cost.
"""

from __future__ import annotations

import contextlib
import io
import logging
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from gentropy.common.session import Session
from gentropy.method.ldsc import run_ldsc_rg_from_arrays

logger = logging.getLogger(__name__)

_RG_SCHEMA = pa.schema([
    pa.field("studyId_1",    pa.string(),  nullable=False),
    pa.field("studyId_2",    pa.string(),  nullable=False),
    pa.field("ancestry",     pa.string(),  nullable=True),
    pa.field("run_status",   pa.string(),  nullable=True),
    pa.field("skip_reason",  pa.string(),  nullable=True),
    pa.field("n_snps",       pa.int64(),   nullable=True),
    pa.field("rg",           pa.float64(), nullable=True),
    pa.field("rg_se",        pa.float64(), nullable=True),
    pa.field("rg_clipped",   pa.bool_(),   nullable=True),
    pa.field("h2_1",         pa.float64(), nullable=True),
    pa.field("h2_1_se",      pa.float64(), nullable=True),
    pa.field("h2_2",         pa.float64(), nullable=True),
    pa.field("h2_2_se",      pa.float64(), nullable=True),
    pa.field("gcov",         pa.float64(), nullable=True),
    pa.field("gcov_se",      pa.float64(), nullable=True),
    pa.field("intercept",    pa.float64(), nullable=True),
    pa.field("intercept_se", pa.float64(), nullable=True),
    pa.field("M_ldsc",       pa.float64(), nullable=True),
])


class GeneticCorrelationChunkStep:
    """Process a chunk of study pairs by reading summary statistics directly.

    For each pair the step:

    1. Reads the harmonised summary statistics for both studies from GCS via
       pyarrow (results are cached across pairs within the task).
    2. Reads the LD-score file for the relevant ancestry (also cached).
    3. Prepares both sumstats: fills N, extracts ref/alt alleles, filters.
    4. Performs a three-way inner join (trait 1 × trait 2 × LD scores) with
       allele-flip handling for trait 2.
    5. Runs LDSC genetic-correlation regression.
    6. Writes all results as a single parquet dataset.
    """

    def __init__(
        self,
        session: Session,
        chunk_manifest_path: str,
        ldscore_base_path: str,
        rg_output_path: str,
        ldscore_template: str = "gnomad_r2.1.1_{ancestry}_hg38.csv.gz",
        twostep: float = 30.0,
        n_blocks: int = 200,
        intercept: float | None = None,
        min_overlap_snps: int = 50,
    ) -> None:
        """Initialise and run the genetic correlation chunk step.

        Args:
            session (Session): Gentropy session object (Hydra compatibility).
            chunk_manifest_path (str): GCS path to the chunk CSV.
                Required columns: ``studyId_1``, ``studyId_2``, ``ancestry``,
                ``sumstats_path_1``, ``sumstats_path_2``.
            ldscore_base_path (str): GCS base directory containing LD-score files.
            rg_output_path (str): Destination parquet path for results.
            ldscore_template (str): LD-score filename template with an ``{ancestry}``
                placeholder (default ``"gnomad_r2.1.1_{ancestry}_hg38.csv.gz"``).
            twostep (float): LDSC two-step chi-square cut-off (default 30.0).
            n_blocks (int): Number of jackknife blocks (default 200).
            intercept (float | None): Fixed cross-trait intercept, or None to estimate.
            min_overlap_snps (int): Minimum overlapping SNPs to run (default 50).
        """
        self.session = session
        self.chunk_manifest_path = chunk_manifest_path
        self.ldscore_base_path = ldscore_base_path.rstrip("/")
        self.rg_output_path = rg_output_path
        self.ldscore_template = ldscore_template
        self.twostep = twostep
        self.n_blocks = n_blocks
        self.intercept = intercept
        self.min_overlap_snps = min_overlap_snps

        self._run()

    def _run(self) -> None:
        """Execute the chunk pipeline."""
        pairs = pd.read_csv(self.chunk_manifest_path)
        required = {"studyId_1", "studyId_2", "ancestry", "sumstats_path_1", "sumstats_path_2"}
        if not required.issubset(pairs.columns):
            raise ValueError(f"Chunk manifest missing columns: {required - set(pairs.columns)}")

        logger.info("Processing %d pairs from %s", len(pairs), self.chunk_manifest_path)

        gcs = pafs.GcsFileSystem()
        sumstats_cache: dict[str, pd.DataFrame] = {}
        ld_cache: dict[str, tuple[pd.DataFrame, float]] = {}
        results: list[dict[str, Any]] = []

        for _, row in pairs.iterrows():
            result = self._process_pair(row, gcs, sumstats_cache, ld_cache)
            results.append(result)

        if not results:
            logger.warning("No results produced for chunk %s", self.chunk_manifest_path)
            return

        result_df = pd.DataFrame(results)
        if "rg_clipped" in result_df.columns:
            result_df["rg_clipped"] = result_df["rg_clipped"].astype("boolean")

        table = pa.Table.from_pandas(result_df, schema=_RG_SCHEMA, safe=False)
        output_fs_path = self.rg_output_path.replace("gs://", "", 1)
        pq.write_to_dataset(table, root_path=output_fs_path, filesystem=gcs)
        logger.info("Wrote %d results to %s", len(results), self.rg_output_path)

    # ── pair processing ───────────────────────────────────────────────────────

    def _process_pair(
        self,
        row: pd.Series,
        gcs: pafs.GcsFileSystem,
        sumstats_cache: dict[str, pd.DataFrame],
        ld_cache: dict[str, tuple[pd.DataFrame, float]],
    ) -> dict[str, Any]:
        """Run LDSC rg for one study pair and return a result dict.

        Args:
            row (pd.Series): Manifest row with studyId_1, studyId_2, ancestry,
                sumstats_path_1, sumstats_path_2.
            gcs (pafs.GcsFileSystem): PyArrow GCS filesystem handle.
            sumstats_cache (dict[str, pd.DataFrame]): In-memory sumstats cache
                keyed by GCS path.
            ld_cache (dict[str, tuple[pd.DataFrame, float]]): In-memory LD score
                cache keyed by ancestry label.

        Returns:
            dict[str, Any]: Result row matching the ``_RG_SCHEMA`` fields.
        """
        s1 = str(row["studyId_1"])
        s2 = str(row["studyId_2"])
        ancestry = str(row["ancestry"])
        p1 = str(row["sumstats_path_1"])
        p2 = str(row["sumstats_path_2"])

        base: dict[str, Any] = {
            "studyId_1": s1, "studyId_2": s2, "ancestry": ancestry,
            "run_status": None, "skip_reason": None, "n_snps": None,
            "rg": None, "rg_se": None, "rg_clipped": None,
            "h2_1": None, "h2_1_se": None, "h2_2": None, "h2_2_se": None,
            "gcov": None, "gcov_se": None, "intercept": None, "intercept_se": None,
            "M_ldsc": None,
        }

        try:
            df1 = self._read_sumstats(p1, gcs, sumstats_cache)
            df2 = self._read_sumstats(p2, gcs, sumstats_cache)
        except Exception as exc:
            return {**base, "run_status": "error", "skip_reason": f"sumstats read error: {exc}"}

        try:
            ld_df, m_ldsc = self._read_ld_scores(ancestry, gcs, ld_cache)
        except Exception as exc:
            return {**base, "run_status": "error", "skip_reason": f"LD score read error: {exc}"}

        try:
            merged = self._merge(df1, df2, ld_df)
        except Exception as exc:
            return {**base, "run_status": "error", "skip_reason": f"merge error: {exc}"}

        if len(merged) < self.min_overlap_snps:
            return {
                **base,
                "run_status": "skipped",
                "skip_reason": f"only {len(merged)} overlapping SNPs (min {self.min_overlap_snps})",
                "n_snps": len(merged),
                "M_ldsc": m_ldsc,
            }

        beta1 = merged["beta1"].values.astype(float)
        se1   = merged["se1"].values.astype(float)
        N1    = merged["n1"].values.astype(float)
        beta2 = merged["beta2"].values.astype(float)
        se2   = merged["se2"].values.astype(float)
        N2    = merged["n2"].values.astype(float)
        ld    = merged["L2"].values.astype(float)

        w_raw = 1.0 / np.maximum(ld, 1.0)
        w_ld  = w_raw / np.mean(w_raw)

        try:
            res = run_ldsc_rg_from_arrays(
                beta1=beta1, se1=se1, N1=N1,
                beta2=beta2, se2=se2, N2=N2,
                ld=ld, w_ld=w_ld,
                M_ldsc_scalar=m_ldsc,
                intercept=self.intercept,
                twostep=self.twostep,
                n_blocks=self.n_blocks,
            )
        except Exception as exc:
            return {**base, "run_status": "error", "skip_reason": f"ldsc error: {exc}",
                    "n_snps": len(merged), "M_ldsc": m_ldsc}

        intercept_se_val: float | None = None
        if res.get("intercept_se") not in (None, "NA"):
            with contextlib.suppress(TypeError, ValueError):
                intercept_se_val = float(res["intercept_se"])

        return {
            **base,
            "run_status":   "success",
            "n_snps":       int(len(merged)),
            "rg":           res["rg"],
            "rg_se":        res["rg_se"],
            "rg_clipped":   bool(res["rg_clipped"]) if res.get("rg_clipped") is not None else None,
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

    # ── data loading (with caching) ───────────────────────────────────────────

    def _read_sumstats(
        self,
        sumstats_path: str,
        gcs: pafs.GcsFileSystem,
        cache: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Read and prepare harmonised summary statistics, caching by path.

        Args:
            sumstats_path (str): GCS path to a harmonised sumstats parquet.
            gcs (pafs.GcsFileSystem): PyArrow GCS filesystem handle.
            cache (dict[str, pd.DataFrame]): Mutable cache populated on first read.

        Returns:
            pd.DataFrame: Prepared sumstats with chromosome, position, ref, alt,
                beta, standardError, sampleSize columns.
        """
        if sumstats_path not in cache:
            fs_path = sumstats_path.replace("gs://", "", 1)
            table = pq.read_table(
                fs_path,
                filesystem=gcs,
                columns=["studyId", "variantId", "chromosome", "position",
                         "beta", "standardError", "sampleSize"],
            )
            df = table.to_pandas()
            cache[sumstats_path] = self._prepare_sumstats(df)
        return cache[sumstats_path]

    def _read_ld_scores(
        self,
        ancestry: str,
        gcs: pafs.GcsFileSystem,
        cache: dict[str, tuple[pd.DataFrame, float]],
    ) -> tuple[pd.DataFrame, float]:
        """Read and prepare the LD-score file for an ancestry, caching by ancestry.

        Args:
            ancestry (str): LD ancestry label (e.g. ``"nfe"``).
            gcs (pafs.GcsFileSystem): PyArrow GCS filesystem handle.
            cache (dict[str, tuple[pd.DataFrame, float]]): Mutable cache populated
                on first read; values are (ld_df, m_ldsc).

        Returns:
            tuple[pd.DataFrame, float]: Prepared LD score DataFrame and the M_ldsc
                scalar (number of LD score SNPs).
        """
        if ancestry not in cache:
            filename = self.ldscore_template.format(ancestry=ancestry)
            fs_path = f"{self.ldscore_base_path}/{filename}".replace("gs://", "", 1)
            with gcs.open_input_file(fs_path) as fh:
                raw = fh.read()
            ld_df = pd.read_csv(io.BytesIO(raw), compression="gzip", sep="\t")
            ld_df = self._prepare_ld_scores(ld_df)
            m_ldsc = float(len(ld_df))
            cache[ancestry] = (ld_df, m_ldsc)
        return cache[ancestry]

    # ── preparation helpers ───────────────────────────────────────────────────

    @staticmethod
    def _prepare_sumstats(df: pd.DataFrame) -> pd.DataFrame:
        """Extract ref/alt from variantId, filter invalid rows, dedup.

        Args:
            df (pd.DataFrame): Raw harmonised sumstats with variantId column.

        Returns:
            pd.DataFrame: Filtered and deduplicated sumstats with ref and alt columns.
        """
        df = df.copy()
        parts = df["variantId"].str.split("_", n=3, expand=True)
        df["ref"] = parts.iloc[:, 2] if parts.shape[1] > 2 else None
        df["alt"] = parts.iloc[:, 3] if parts.shape[1] > 3 else None
        df["sampleSize"] = pd.to_numeric(df["sampleSize"], errors="coerce")
        df = df[
            df["beta"].notna()
            & df["standardError"].notna()
            & (df["standardError"] > 0)
            & df["sampleSize"].notna()
            & (df["sampleSize"] > 0)
            & df["chromosome"].notna()
            & df["position"].notna()
            & df["ref"].notna()
            & df["alt"].notna()
        ]
        return df.drop_duplicates(["chromosome", "position", "ref", "alt"])

    @staticmethod
    def _prepare_ld_scores(ld_df: pd.DataFrame) -> pd.DataFrame:
        """Standardise column names and types, filter nulls.

        Args:
            ld_df (pd.DataFrame): Raw LD score DataFrame as read from the CSV.

        Returns:
            pd.DataFrame: Cleaned LD scores with chromosome, position, ref, alt, L2.
        """
        if "BP_hg38" in ld_df.columns:
            ld_df = ld_df.rename(columns={"BP_hg38": "position"})
        if "CHR" in ld_df.columns:
            ld_df = ld_df.rename(columns={"CHR": "chromosome"})
        required = {"chromosome", "position", "ref", "alt", "L2"}
        missing = required - set(ld_df.columns)
        if missing:
            raise ValueError(f"LD score file missing columns: {missing}")
        ld_df = ld_df[list(required)].copy()
        ld_df["position"] = pd.to_numeric(ld_df["position"], errors="coerce")
        ld_df["chromosome"] = ld_df["chromosome"].astype(str)
        ld_df["L2"] = pd.to_numeric(ld_df["L2"], errors="coerce")
        return ld_df[ld_df["L2"].notna()].drop_duplicates(["chromosome", "position", "ref", "alt"])

    @staticmethod
    def _merge(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        ld_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Three-way inner join: trait1 × trait2 × LD scores with allele-flip for trait2.

        Args:
            df1 (pd.DataFrame): Prepared sumstats for trait 1.
            df2 (pd.DataFrame): Prepared sumstats for trait 2.
            ld_df (pd.DataFrame): LD scores with chromosome, position, ref, alt, L2.

        Returns:
            pd.DataFrame: Merged DataFrame with beta1, se1, n1, beta2, se2, n2, L2 columns.
        """
        join_cols = ["chromosome", "position", "ref", "alt"]

        t1 = df1[join_cols + ["beta", "standardError", "sampleSize"]].rename(
            columns={"beta": "beta1", "standardError": "se1", "sampleSize": "n1"}
        )
        t2 = df2[join_cols + ["beta", "standardError", "sampleSize"]].rename(
            columns={"beta": "beta2", "standardError": "se2", "sampleSize": "n2"}
        )

        # Allow allele flips in trait 2: ref↔alt swap with beta sign flip
        t2_flip = t2.rename(columns={"ref": "alt", "alt": "ref"}).copy()
        t2_flip["beta2"] = -t2_flip["beta2"]

        t2_combined = (
            pd.concat([t2, t2_flip], ignore_index=True)
            .drop_duplicates(join_cols)
        )

        merged = (
            t1
            .merge(t2_combined, on=join_cols, how="inner")
            .merge(ld_df[join_cols + ["L2"]], on=join_cols, how="inner")
            .drop_duplicates(join_cols)
        )
        return merged
