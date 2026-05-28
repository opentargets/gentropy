"""LDSC-style genetic correlation estimation on arrays."""
from __future__ import annotations

from typing import Any

import numpy as np

from gentropy.method.ldsc.regression import GeneticCov, Hsq
from gentropy.method.ldsc.utils import _as_float_or_none


def run_ldsc_rg_from_arrays(
    beta1: np.ndarray,
    se1: np.ndarray,
    N1: np.ndarray,
    beta2: np.ndarray,
    se2: np.ndarray,
    N2: np.ndarray,
    ld: np.ndarray,
    w_ld: np.ndarray,
    M_ldsc_scalar: float,
    intercept: float | None = None,
    twostep: float | None = 30.0,
    n_blocks: int = 200,
) -> dict[str, Any]:
    """Run LDSC-style genetic correlation regression directly on arrays.

    Estimates the genetic correlation between two traits by:
    1. Running heritability regression (Hsq) on each trait independently.
    2. Running genetic covariance regression (GeneticCov) on z1*z2.
    3. Computing rg = gcov / sqrt(h2_1 * h2_2) with delta-method SE.

    Args:
        beta1 (np.ndarray): Effect estimates for trait 1, shape (n_snp,).
        se1 (np.ndarray): Standard errors for trait 1, shape (n_snp,).
        N1 (np.ndarray): Sample sizes for trait 1, shape (n_snp,).
        beta2 (np.ndarray): Effect estimates for trait 2, shape (n_snp,).
        se2 (np.ndarray): Standard errors for trait 2, shape (n_snp,).
        N2 (np.ndarray): Sample sizes for trait 2, shape (n_snp,).
        ld (np.ndarray): Per-SNP LD scores, shape (n_snp,).
        w_ld (np.ndarray): Per-SNP LD scores used for weighting, shape (n_snp,).
        M_ldsc_scalar (float): Number of SNPs used to compute LD scores.
        intercept (float | None): Fixed cross-trait intercept. If None, estimated.
        twostep (float | None): Two-step chi-square cut-off. Applied to |z1*z2|.
        n_blocks (int): Number of jackknife blocks.

    Returns:
        dict[str, Any]: Keys: rg, rg_se, gcov, gcov_se, h2_1, h2_1_se,
            h2_2, h2_2_se, intercept, intercept_se, n_snps.
    """
    for arr in (beta1, se1, N1, beta2, se2, N2, ld, w_ld):
        arr = np.asarray(arr, dtype=float)
        if arr.ndim != 1:
            raise ValueError("All input arrays must be 1D.")

    beta1 = np.asarray(beta1, dtype=float)
    se1 = np.asarray(se1, dtype=float)
    N1 = np.asarray(N1, dtype=float)
    beta2 = np.asarray(beta2, dtype=float)
    se2 = np.asarray(se2, dtype=float)
    N2 = np.asarray(N2, dtype=float)
    ld = np.asarray(ld, dtype=float)
    w_ld = np.asarray(w_ld, dtype=float)

    z1 = beta1 / se1
    z2 = beta2 / se2
    n = z1.shape[0]

    M_mat = np.array([[float(M_ldsc_scalar)]])
    x = ld.reshape((n, 1))
    w = w_ld.reshape((n, 1))

    # Step 1: Estimate h2 for each trait
    def _run_hsq(z: np.ndarray, N: np.ndarray) -> tuple[float, float]:
        """Run Hsq regression on a single trait and return (h2, h2_se).

        Args:
            z (np.ndarray): Z-scores for the trait, shape (n_snp,).
            N (np.ndarray): Sample sizes, shape (n_snp,).

        Returns:
            tuple[float, float]: Heritability estimate and its standard error.
        """
        chisq = z ** 2
        y_h = chisq.reshape((n, 1))
        N_mat = N.reshape((n, 1))
        step_cutoff = twostep if intercept is None else None
        hsqhat = Hsq(
            y_h, x, w, N_mat, M_mat,
            n_blocks=n_blocks,
            intercept=intercept,
            twostep=step_cutoff,
        )
        h2 = float(max(hsqhat.tot, 0.0))
        h2_se = float(hsqhat.tot_se)
        return h2, h2_se

    h2_1, h2_1_se = _run_hsq(z1, N1)
    h2_2, h2_2_se = _run_hsq(z2, N2)

    # Step 2: Genetic covariance regression
    y_gc = (z1 * z2).reshape((n, 1))
    N1_mat = N1.reshape((n, 1))
    N2_mat = N2.reshape((n, 1))

    step_cutoff_gc = twostep if intercept is None else None

    gencov = GeneticCov(
        y_gc, x, w, N1_mat, N2_mat, M_mat,
        n_blocks=n_blocks,
        intercept=intercept,
        hsq1=h2_1,
        hsq2=h2_2,
        twostep=step_cutoff_gc,
    )

    gcov = float(gencov.tot)
    gcov_se = float(gencov.tot_se)
    intercept_out = _as_float_or_none(gencov.intercept)
    intercept_se_out = gencov.intercept_se

    # Step 3: rg and delta-method SE
    denom_sq = h2_1 * h2_2
    if denom_sq > 0:
        rg_raw = gcov / np.sqrt(denom_sq)
        # Delta method: Var(rg) ≈ rg^2 * [(gcov_se/gcov)^2 + 0.25*(h2_1_se/h2_1)^2 + 0.25*(h2_2_se/h2_2)^2]
        terms = []
        if gcov != 0:
            terms.append((gcov_se / gcov) ** 2)
        if h2_1 > 0:
            terms.append(0.25 * (h2_1_se / h2_1) ** 2)
        if h2_2 > 0:
            terms.append(0.25 * (h2_2_se / h2_2) ** 2)
        rg_var = rg_raw ** 2 * sum(terms) if terms else float("nan")
        rg_se = float(np.sqrt(max(rg_var, 0.0)))
        rg = float(np.clip(rg_raw, -1.0, 1.0))
        rg_clipped = abs(rg_raw) > 1.0
    else:
        rg = float("nan")
        rg_se = float("nan")
        rg_clipped = False

    return {
        "rg": rg,
        "rg_se": rg_se,
        "rg_clipped": rg_clipped,
        "gcov": gcov,
        "gcov_se": gcov_se,
        "h2_1": h2_1,
        "h2_1_se": h2_1_se,
        "h2_2": h2_2,
        "h2_2_se": h2_2_se,
        "intercept": intercept_out,
        "intercept_se": intercept_se_out,
        "n_snps": n,
    }
