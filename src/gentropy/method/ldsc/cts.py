"""LDSC cell-type-specific (partitioned) heritability enrichment wrapper.

This implements the statistical core of LDSC ``--h2-cts`` (as used by
`CELLECT <https://github.com/perslab/CELLECT>`_): a stratified LD Score
regression in which the GWAS chi-square statistics are regressed on a set of
baseline annotation LD scores plus a single focal (cell-type) annotation LD
score. The regression coefficient of the focal annotation, together with its
block-jackknife standard error, yields a one-sided p-value quantifying whether
the cell-type annotation is enriched for trait heritability.
"""
from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any

import numpy as np

from gentropy.method.ldsc.regression import Hsq
from gentropy.method.ldsc.utils import _as_float_or_none

_LD_POPULATION_MAP = {
    "afr": "afr",
    "amr": "amr",
    "eas": "eas",
    "fin": "fin",
    "nfe": "nfe",
}


def _extract_ld_population_and_weight(entry: Any) -> tuple[Any, Any]:
    """Extract population and relative sample-size fields from one record."""
    if isinstance(entry, Mapping):
        population = entry.get("ldPopulation") or entry.get("population")
        weight = entry.get("relativeSampleSize")
        if weight is None:
            weight = entry.get("weight")
        if weight is None:
            weight = entry.get("proportion")
        return population, weight

    population = getattr(entry, "ldPopulation", None) or getattr(
        entry, "population", None
    )
    weight = getattr(entry, "relativeSampleSize", None)
    if weight is None:
        weight = getattr(entry, "weight", None)
    if weight is None:
        weight = getattr(entry, "proportion", None)
    return population, weight


def infer_ld_ancestry(ld_population_structure: Any) -> str:
    """Select the largest-population LDSC reference from study metadata.

    ``ldPopulationStructure`` is an array of population records containing an
    ``ldPopulation`` and a ``relativeSampleSize``. Duplicate population records
    are aggregated before choosing the largest plurality. Ties retain the
    existing deterministic policy: prefer NFE, then the lexicographically first
    canonical population.

    Args:
        ld_population_structure: Iterable of mappings or Spark Row-like objects.

    Returns:
        Canonical LDSC population label.

    Raises:
        ValueError: If no recognised population has a usable weight.
        TypeError: If the value is not iterable.
    """
    if ld_population_structure is None:
        raise ValueError("ldPopulationStructure is None")
    if isinstance(ld_population_structure, (str, bytes)) or not isinstance(
        ld_population_structure, Iterable
    ):
        raise TypeError(
            "ldPopulationStructure must be an iterable of population records"
        )

    aggregate: dict[str, float] = {}
    for entry in ld_population_structure:
        population, weight = _extract_ld_population_and_weight(entry)

        canonical = _LD_POPULATION_MAP.get(str(population).strip().lower()) if population else None
        try:
            numeric_weight = float(weight)
        except (TypeError, ValueError):
            numeric_weight = float("nan")
        # Relative sample sizes are proportions, so zero/negative and non-finite
        # values do not provide usable evidence for choosing a reference panel.
        if canonical is None or not math.isfinite(numeric_weight) or numeric_weight <= 0:
            continue
        aggregate[canonical] = aggregate.get(canonical, 0.0) + numeric_weight

    if not aggregate:
        raise ValueError(
            "Could not map any populations from ldPopulationStructure: "
            f"{ld_population_structure}"
        )

    maximum = max(aggregate.values())
    tied = [population for population, weight in aggregate.items() if weight == maximum]
    return "nfe" if "nfe" in tied else sorted(tied)[0]


def _one_sided_p_value(z: float) -> float:
    """Compute the one-sided (upper-tail) normal p-value for a z-score.

    This tests the alternative hypothesis that the coefficient is positive,
    matching LDSC ``--h2-cts`` which reports ``P(Z > z)``.

    Args:
        z (float): Standard normal z-score.

    Returns:
        float: Upper-tail probability ``P(Z > z)``, or ``float('nan')`` if
        ``z`` is NaN. Infinite z-scores return 0.0 (``+inf``) or 1.0
        (``-inf``).
    """
    if math.isnan(z):
        return float("nan")
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def run_ldsc_cts_from_arrays(
    beta: np.ndarray,
    se: np.ndarray,
    N: np.ndarray,
    ref_ld: np.ndarray,
    w_ld: np.ndarray,
    M_annot: np.ndarray,
    focal_index: int = -1,
    intercept: float | None = None,
    n_blocks: int = 200,
) -> dict[str, Any]:
    """Run stratified LDSC (``--h2-cts``) directly on aligned arrays.

    All per-SNP arrays must already be matched and aligned (same SNP order)
    across the summary statistics, the baseline/focal LD scores, and the
    regression-weight LD scores.

    The design matrix ``ref_ld`` contains one column per annotation. Typically
    these are the baseline model annotations, an "all genes" control
    annotation, and finally the focal cell-type annotation. The coefficient
    (``tau``) of the focal annotation and its jackknife standard error are used
    to compute a one-sided p-value.

    Args:
        beta (np.ndarray): Per-SNP effect estimates of shape (n_snp,).
        se (np.ndarray): Per-SNP standard errors of shape (n_snp,).
        N (np.ndarray): Per-SNP sample sizes of shape (n_snp,).
        ref_ld (np.ndarray): Annotation LD-score design matrix of shape
            (n_snp, n_annot). The focal (cell-type) annotation is selected by
            ``focal_index``.
        w_ld (np.ndarray): Regression-weight LD scores of shape (n_snp,), used
            to build the LDSC heteroskedasticity/overcounting weights.
        M_annot (np.ndarray): Per-annotation sizes of shape (n_annot,), for
            example the number of reference SNPs contributing to each
            annotation (``.l2.M_5_50``). For continuous annotations this is the
            sum of the annotation values over the reference panel.
        focal_index (int): Column index in ``ref_ld`` of the focal cell-type
            annotation. Negative indices are supported (default ``-1``, the
            last column).
        intercept (float | None): Fixed LDSC intercept. If None, the intercept
            is estimated.
        n_blocks (int): Number of block-jackknife blocks.

    Returns:
        dict[str, Any]: Dictionary with keys:
            "coefficients", "coefficients_se" (per-annotation arrays),
            "coefficients_p" (per-annotation one-sided p-values),
            "coefficient", "coefficient_se", "coefficient_z",
            "coefficient_p_value" (focal annotation),
            "h2", "h2_se", "intercept", "intercept_se",
            "mean_chisq", "lambda_gc", "n_snps", "n_annot", "focal_index".

    Raises:
        ValueError: If array dimensions are inconsistent or ``focal_index`` is
            out of range.
    """
    beta = np.asarray(beta, dtype=float)
    se = np.asarray(se, dtype=float)
    N = np.asarray(N, dtype=float)
    ref_ld = np.asarray(ref_ld, dtype=float)
    w_ld = np.asarray(w_ld, dtype=float)
    M_annot = np.asarray(M_annot, dtype=float)

    if not (beta.ndim == se.ndim == N.ndim == w_ld.ndim == 1):
        raise ValueError("beta, se, N and w_ld must be 1D arrays.")
    if ref_ld.ndim != 2:
        raise ValueError("ref_ld must be a 2D array of shape (n_snp, n_annot).")

    n_snp, n_annot = ref_ld.shape

    if not (beta.shape[0] == se.shape[0] == N.shape[0] == w_ld.shape[0] == n_snp):
        raise ValueError(
            "beta, se, N, w_ld and ref_ld must share the same number of SNPs."
        )
    if M_annot.ndim != 1 or M_annot.shape[0] != n_annot:
        raise ValueError("M_annot must be a 1D array of length n_annot.")

    resolved_focal = focal_index + n_annot if focal_index < 0 else focal_index
    if not 0 <= resolved_focal < n_annot:
        raise ValueError(
            f"focal_index {focal_index} is out of range for n_annot={n_annot}."
        )

    z_score = beta / se
    chisq = z_score**2

    y = chisq.reshape((n_snp, 1))
    x = ref_ld
    w = w_ld.reshape((n_snp, 1))
    N_mat = N.reshape((n_snp, 1))
    M_mat = M_annot.reshape((1, n_annot))

    old_weights = n_annot > 1

    hsqhat = Hsq(
        y,
        x,
        w,
        N_mat,
        M_mat,
        n_blocks=n_blocks,
        intercept=intercept,
        slow=False,
        twostep=None,
        old_weights=old_weights,
    )

    coefficients = np.asarray(hsqhat.coef, dtype=float).reshape(-1)
    coefficients_se = np.asarray(hsqhat.coef_se, dtype=float).reshape(-1)

    with np.errstate(divide="ignore", invalid="ignore"):
        z_scores = np.where(coefficients_se > 0, coefficients / coefficients_se, np.nan)
    coefficients_p = np.array(
        [_one_sided_p_value(float(z)) for z in z_scores], dtype=float
    )

    focal_coef = float(coefficients[resolved_focal])
    focal_se = float(coefficients_se[resolved_focal])
    focal_z = focal_coef / focal_se if focal_se > 0 else float("nan")

    return {
        "coefficients": coefficients,
        "coefficients_se": coefficients_se,
        "coefficients_p": coefficients_p,
        "coefficient": focal_coef,
        "coefficient_se": focal_se,
        "coefficient_z": focal_z,
        "coefficient_p_value": _one_sided_p_value(focal_z),
        "h2": float(hsqhat.tot),
        "h2_se": float(hsqhat.tot_se),
        "intercept": _as_float_or_none(hsqhat.intercept),
        "intercept_se": float(hsqhat.intercept_se)
        if not isinstance(hsqhat.intercept_se, str)
        else None,
        "mean_chisq": float(hsqhat.mean_chisq),
        "lambda_gc": float(hsqhat.lambda_gc),
        "n_snps": int(n_snp),
        "n_annot": int(n_annot),
        "focal_index": int(resolved_focal),
    }
