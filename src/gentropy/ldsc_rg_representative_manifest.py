"""Step to select representative studies and generate a pairwise genetic correlation manifest."""

from __future__ import annotations

import logging
from itertools import combinations
from typing import Any

import pandas as pd
from pyspark.sql import functions as F

from gentropy.common.session import Session

logger = logging.getLogger(__name__)

ALLOWED_ANALYSIS_FLAGS: frozenset[str] = frozenset({"exwas", "wgsgwas", "metabolite"})

DISQUALIFYING_QC_FLAGS: frozenset[str] = frozenset({
    "Harmonized summary statistics are not available or empty",
    "The PZ QC check values are not within the expected range",
    "The mean beta QC check value is not within the expected range",
    "The GC lambda value is not within the expected range",
    "Case-case study design",
    "The identifier of this study is not unique",
})


class RepresentativeStudyManifestStep:
    """Select representative studies per (diseaseId, ancestry) and generate a pairwise manifest.

    Reads the study index and heritability estimates, applies quality filters,
    selects one representative study per (diseaseId, ld_ancestry) cell ranked by
    effective sample size, and writes a pairs manifest CSV for downstream genetic
    correlation analysis.

    If ``previous_manifest_path`` is provided the step also writes a delta manifest
    containing only pairs that are new relative to the previous release (at least one
    study is a newly selected representative), and a deprecation report listing pairs
    from the previous release that are no longer current because a study was replaced.
    """

    def __init__(
        self,
        session: Session,
        study_index_path: str,
        heritability_estimates_path: str,
        sumstats_base_path: str,
        manifest_output_path: str,
        previous_manifest_path: str | None = None,
        min_h2_z_squared: float = 4.0,
        min_neff: int = 10_000,
        min_h2: float = 0.0,
        max_h2: float = 1.0,
        min_lambda_gc: float = 0.8,
        max_lambda_gc: float = 2.5,
        include_ancestries: list[str] | None = None,
    ) -> None:
        """Initialise and run the representative study manifest step.

        Args:
            session (Session): Gentropy session object.
            study_index_path (str): Path to the GWAS study index parquet.
            heritability_estimates_path (str): Path to heritability estimates parquet(s).
            sumstats_base_path (str): Base GCS path for harmonised summary statistics.
                Per-study path is constructed as ``{sumstats_base_path}/{studyId}``.
            manifest_output_path (str): Path to write the full pairs manifest CSV.
            previous_manifest_path (str | None): Optional path to the previous release's
                full manifest CSV.  When provided, a delta manifest and deprecation report
                are written alongside the full manifest.
            min_h2_z_squared (float): Minimum (h²/SE)² threshold (default 4.0, i.e. |Z| ≥ 2).
            min_neff (int): Minimum effective sample size (default 10_000).
            min_h2 (float): Minimum h² point estimate, exclusive (default 0.0).
            max_h2 (float): Maximum h² point estimate, exclusive (default 1.0).
            min_lambda_gc (float): Minimum genomic inflation factor (default 0.8).
            max_lambda_gc (float): Maximum genomic inflation factor (default 2.5).
            include_ancestries (list[str] | None): Restrict to these LD ancestries.
                Defaults to ``["nfe"]``.
        """
        self.session = session
        self.study_index_path = study_index_path
        self.heritability_estimates_path = heritability_estimates_path
        self.sumstats_base_path = sumstats_base_path.rstrip("/")
        self.manifest_output_path = manifest_output_path
        self.previous_manifest_path = previous_manifest_path
        self.min_h2_z_squared = min_h2_z_squared
        self.min_neff = min_neff
        self.min_h2 = min_h2
        self.max_h2 = max_h2
        self.min_lambda_gc = min_lambda_gc
        self.max_lambda_gc = max_lambda_gc
        self.include_ancestries: list[str] = (
            include_ancestries if include_ancestries is not None else ["nfe"]
        )
        self._run()

    def _run(self) -> None:
        """Execute the representative study selection pipeline."""
        study_pd = self._load_and_filter_studies()
        logger.info("Studies after initial filters: %d", len(study_pd))

        h2_pd = self._load_heritability()
        logger.info("Heritability successful runs: %d", len(h2_pd))

        merged = self._merge_and_apply_h2_filters(study_pd, h2_pd)
        logger.info("Studies after h² filters: %d", len(merged))

        representatives = self._select_representatives(merged)
        logger.info(
            "Representative studies: %d unique across %d (diseaseId, ancestry) cells",
            representatives["studyId"].nunique(),
            len(representatives),
        )

        full_pairs = self._generate_pairs(representatives)
        logger.info("Total pairwise combinations: %d", len(full_pairs))

        full_pairs.to_csv(self.manifest_output_path, index=False)
        logger.info("Full manifest written to %s", self.manifest_output_path)

        if self.previous_manifest_path is not None:
            self._write_delta(full_pairs, representatives)

    # ── delta / deprecation ───────────────────────────────────────────────────

    def _write_delta(
        self,
        full_pairs: pd.DataFrame,
        representatives: pd.DataFrame,
    ) -> None:
        """Write a delta manifest (new pairs only) and a deprecation report.

        The delta manifest contains pairs where at least one study is newly selected
        as a representative — i.e. not present in the previous release's manifest.
        The deprecation report lists pairs from the previous release that are no longer
        current because one or both studies have been replaced.

        Args:
            full_pairs (pd.DataFrame): Current full pairs manifest.
            representatives (pd.DataFrame): Current representative studies table.
        """
        try:
            prev = pd.read_csv(self.previous_manifest_path)
        except Exception as exc:
            logger.warning("Could not read previous manifest (%s) — skipping delta.", exc)
            return

        current_studies = set(representatives["studyId"].unique())
        prev_studies = set(prev["studyId_1"].unique()) | set(prev["studyId_2"].unique())
        deprecated_studies = prev_studies - current_studies
        new_studies = current_studies - prev_studies

        logger.info(
            "Delta: %d new representative studies, %d deprecated",
            len(new_studies), len(deprecated_studies),
        )

        # New pairs: not present in the previous manifest
        prev_pair_keys = set(zip(prev["studyId_1"], prev["studyId_2"]))
        delta_pairs = full_pairs[
            ~full_pairs.apply(
                lambda r: (r["studyId_1"], r["studyId_2"]) in prev_pair_keys, axis=1
            )
        ].copy()
        logger.info("New pairs to compute: %d", len(delta_pairs))

        # Deprecated pairs: previous pairs where either study is no longer representative
        deprecated_mask = (
            prev["studyId_1"].isin(deprecated_studies)
            | prev["studyId_2"].isin(deprecated_studies)
        )
        deprecated_pairs = prev[deprecated_mask].copy()
        deprecated_pairs["deprecated_because"] = deprecated_pairs.apply(
            lambda r: (
                f"{r['studyId_1']} replaced"
                if r["studyId_1"] in deprecated_studies
                else f"{r['studyId_2']} replaced"
            ),
            axis=1,
        )
        logger.info("Pairs deprecated in this release: %d", len(deprecated_pairs))

        base = self.manifest_output_path.rsplit(".", 1)[0]
        delta_path = f"{base}_delta.csv"
        deprecated_path = f"{base}_deprecated.csv"

        delta_pairs.to_csv(delta_path, index=False)
        deprecated_pairs.to_csv(deprecated_path, index=False)
        logger.info("Delta manifest → %s", delta_path)
        logger.info("Deprecation report → %s", deprecated_path)

    # ── data loading ──────────────────────────────────────────────────────────

    def _load_and_filter_studies(self) -> pd.DataFrame:
        """Load study index and apply pre-h² quality filters.

        Returns:
            pd.DataFrame: Studies passing initial quality and ancestry filters.
        """
        study_pd: pd.DataFrame = (
            self.session.spark.read.parquet(self.study_index_path)
            .select(
                "studyId",
                "studyType",
                "traitFromSourceMappedIds",
                "nCases",
                "nControls",
                "nSamples",
                "ldPopulationStructure",
                "analysisFlags",
                "qualityControls",
                "hasSumstats",
            )
            .toPandas()
        )

        study_pd["neff"] = study_pd.apply(self._compute_neff, axis=1)
        study_pd["ld_ancestry"] = study_pd["ldPopulationStructure"].apply(
            self._safe_infer_ancestry
        )
        study_pd["ldsc_ok"] = study_pd["analysisFlags"].apply(self._is_ldsc_compatible)
        study_pd["bad_qc"] = study_pd["qualityControls"].apply(self._has_disqualifying_qc)

        df = study_pd[
            (study_pd["studyType"] == "gwas")
            & (study_pd["hasSumstats"] == True)  # noqa: E712
            & (~study_pd["bad_qc"])
            & (study_pd["ldsc_ok"])
            & (study_pd["neff"].notna())
            & (study_pd["neff"] >= self.min_neff)
            & (study_pd["ld_ancestry"].notna())
        ].copy()

        if self.include_ancestries:
            df = df[df["ld_ancestry"].isin(self.include_ancestries)]

        return df

    def _load_heritability(self) -> pd.DataFrame:
        """Load successful heritability estimates.

        Returns:
            pd.DataFrame: Heritability rows with runStatus == 'success'.
        """
        return (
            self.session.spark.read.parquet(self.heritability_estimates_path)
            .filter(F.col("runStatus") == "success")
            .select("studyId", "h2", "h2_se", "lambda_gc", "n_snps_used")
            .toPandas()
        )

    # ── filtering and selection ───────────────────────────────────────────────

    def _merge_and_apply_h2_filters(
        self, study_pd: pd.DataFrame, h2_pd: pd.DataFrame
    ) -> pd.DataFrame:
        """Join studies with heritability estimates and apply h² quality thresholds.

        Args:
            study_pd (pd.DataFrame): Studies passing pre-h² filters.
            h2_pd (pd.DataFrame): Heritability estimates for successful runs.

        Returns:
            pd.DataFrame: Inner-joined studies passing all h² and lambda_gc filters.
        """
        merged = study_pd.merge(h2_pd, on="studyId", how="inner")
        merged["h2_z_sq"] = (
            merged["h2"] / merged["h2_se"].replace(0, float("nan"))
        ) ** 2

        merged = merged[
            (merged["h2"] > self.min_h2)
            & (merged["h2"] < self.max_h2)
            & (merged["h2_z_sq"] >= self.min_h2_z_squared)
        ]

        lambda_ok = merged["lambda_gc"].isna() | (
            (merged["lambda_gc"] >= self.min_lambda_gc)
            & (merged["lambda_gc"] <= self.max_lambda_gc)
        )
        return merged[lambda_ok].copy()

    def _select_representatives(self, df: pd.DataFrame) -> pd.DataFrame:
        """Explode disease IDs and take the top-neff study per (diseaseId, ld_ancestry).

        Args:
            df (pd.DataFrame): Studies passing all quality filters.

        Returns:
            pd.DataFrame: One representative study per (diseaseId, ld_ancestry) cell.
        """
        df = df.copy()
        df["_disease_list"] = df.apply(self._primary_disease_ids, axis=1)
        exploded = (
            df.explode("_disease_list")
            .rename(columns={"_disease_list": "diseaseId"})
        )
        exploded = exploded[
            exploded["diseaseId"].notna() & (exploded["diseaseId"] != "")
        ]

        # Rank within each cell: highest neff first, h2_z_sq as tiebreaker
        exploded = exploded.sort_values(["neff", "h2_z_sq"], ascending=[False, False])
        exploded["rank_in_cell"] = (
            exploded.groupby(["diseaseId", "ld_ancestry"])["neff"]
            .rank(method="first", ascending=False)
            .astype(int)
        )
        return exploded[exploded["rank_in_cell"] == 1].copy()

    def _generate_pairs(self, representatives: pd.DataFrame) -> pd.DataFrame:
        """Generate all pairwise combinations within each LD ancestry.

        Args:
            representatives (pd.DataFrame): One representative study per
                (diseaseId, ld_ancestry) cell.

        Returns:
            pd.DataFrame: All pairwise combinations with columns ancestry,
                studyId_1, studyId_2, sumstats_path_1, sumstats_path_2.
        """
        studies_per_ancestry: dict[str, list[str]] = (
            representatives.groupby("ld_ancestry")["studyId"]
            .apply(lambda x: sorted(x.unique().tolist()))
            .to_dict()
        )

        rows: list[dict[str, str]] = []
        for ancestry, study_ids in sorted(studies_per_ancestry.items()):
            n_pairs = len(study_ids) * (len(study_ids) - 1) // 2
            logger.info(
                "Ancestry %s: %d studies → %d pairs", ancestry, len(study_ids), n_pairs
            )
            for s1, s2 in combinations(sorted(study_ids), 2):
                rows.append({
                    "ancestry": ancestry,
                    "studyId_1": s1,
                    "studyId_2": s2,
                    "sumstats_path_1": f"{self.sumstats_base_path}/{s1}",
                    "sumstats_path_2": f"{self.sumstats_base_path}/{s2}",
                })
        return pd.DataFrame(rows)

    # ── static helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _primary_disease_ids(row: pd.Series) -> list[str]:
        """Return disease IDs from traitFromSourceMappedIds.

        Args:
            row (pd.Series): A study index row containing traitFromSourceMappedIds.

        Returns:
            list[str]: Non-empty disease identifier strings, or an empty list.
        """
        mapped = row.get("traitFromSourceMappedIds")
        if mapped and len(mapped) > 0:
            return [d for d in mapped if d]
        return []

    @staticmethod
    def _compute_neff(row: pd.Series) -> float | None:
        """Compute effective N: 4·K·C/(K+C) for case-control, else nSamples.

        Args:
            row (pd.Series): A study index row containing nCases, nControls, nSamples.

        Returns:
            float | None: Effective sample size, or None when it cannot be computed.
        """
        k = row.get("nCases")
        c = row.get("nControls")
        if pd.notna(k) and pd.notna(c) and k > 0 and c > 0:
            return 4.0 * float(k) * float(c) / (float(k) + float(c))
        n = row.get("nSamples")
        return float(n) if pd.notna(n) and n > 0 else None

    @staticmethod
    def _extract_pop_weight(entry: Any) -> tuple[str | None, float | None]:
        """Extract population label and weight from a single population structure entry.

        Args:
            entry (Any): A population structure entry (namedtuple, Row, or dict).

        Returns:
            tuple[str | None, float | None]: Population label and relative weight,
                either of which may be None if not present in the entry.
        """
        pop: str | None = None
        weight: float | None = None
        if hasattr(entry, "ldPopulation"):
            pop = entry.ldPopulation
        elif hasattr(entry, "population"):
            pop = entry.population
        if hasattr(entry, "relativeSampleSize"):
            weight = entry.relativeSampleSize
        elif hasattr(entry, "proportion"):
            weight = entry.proportion
        if isinstance(entry, dict):
            pop = pop or entry.get("ldPopulation") or entry.get("population")
            weight = weight or entry.get("relativeSampleSize") or entry.get("proportion")
        return pop, weight

    @staticmethod
    def _safe_infer_ancestry(ld_pop_struct: Any) -> str | None:
        """Infer canonical LD ancestry from ldPopulationStructure; return None on failure.

        Args:
            ld_pop_struct (Any): ldPopulationStructure column value (iterable or None).

        Returns:
            str | None: Canonical ancestry label (e.g. 'nfe', 'eas'), or None when
                the ancestry cannot be determined or the input is None.
        """
        if ld_pop_struct is None:
            return None
        pop_map = {"afr": "afr", "amr": "amr", "eas": "eas", "fin": "fin", "nfe": "nfe"}
        agg: dict[str, float] = {}
        try:
            for entry in ld_pop_struct:
                pop, weight = RepresentativeStudyManifestStep._extract_pop_weight(entry)
                if pop is None or weight is None:
                    continue
                canonical = pop_map.get(str(pop).strip().lower())
                if canonical:
                    agg[canonical] = agg.get(canonical, 0.0) + float(weight)
        except Exception:
            return None
        if not agg:
            return None
        max_weight = max(agg.values())
        tied = [p for p, w in agg.items() if w == max_weight]
        return "nfe" if "nfe" in tied else sorted(tied)[0]

    @staticmethod
    def _is_ldsc_compatible(analysis_flags: Any) -> bool:
        """True when analysisFlags are a subset of the LDSC-allowed set (or absent).

        Args:
            analysis_flags (Any): analysisFlags column value (iterable or None).

        Returns:
            bool: True if all flags are LDSC-compatible or no flags are present.
        """
        if analysis_flags is None:
            return True
        flags = {str(f).strip().lower() for f in analysis_flags if f is not None}
        return flags.issubset(ALLOWED_ANALYSIS_FLAGS)

    @staticmethod
    def _has_disqualifying_qc(qc_flags: Any) -> bool:
        """True when any qualityControls entry is in the disqualifying set.

        Args:
            qc_flags (Any): qualityControls column value (iterable or None).

        Returns:
            bool: True if any disqualifying QC flag is present.
        """
        if qc_flags is None:
            return False
        return bool({str(f) for f in qc_flags if f} & DISQUALIFYING_QC_FLAGS)
