"""LDSC-style SNP-heritability convenience wrapper."""
from __future__ import annotations

from typing import Any

import numpy as np

from gentropy.method.ldsc.regression import Hsq
from gentropy.method.ldsc.utils import _as_float_or_none


def run_ldsc_h2_from_arrays(
    beta: np.ndarray,
    se: np.ndarray,
    N: np.ndarray,
    ld: np.ndarray,
    w_ld: np.ndarray,
    M_ldsc_scalar: float,
    intercept: float | None = None,
    twostep: float | None = 30.0,
    n_blocks: int = 200,
) -> dict[str, Any]:
    """Run LDSC-style SNP-heritability regression directly on arrays.

    This is a convenience wrapper for the `Hsq` class that takes 1D NumPy
    arrays and returns a dictionary of LDSC outputs similar to the original
    LDSC implementation.

    Args:
        beta (np.ndarray): Per-SNP effect estimates of shape (n_snp,).
        se (np.ndarray): Per-SNP standard errors of shape (n_snp,).
        N (np.ndarray): Per-SNP sample sizes of shape (n_snp,).
        ld (np.ndarray): Per-SNP LD scores of shape (n_snp,).
        w_ld (np.ndarray): Per-SNP LD scores used for weighting of shape
            (n_snp,).
        M_ldsc_scalar (float): Scalar `M` from LDSC `.l2.M` or `.l2.M_5_50`,
            representing the number of SNPs used to compute LD scores.
        intercept (float | None): Fixed LDSC intercept. If None, the intercept
            is estimated.
        twostep (float | None): If not None (and intercept is free), use
            LDSC's two-step procedure with this chi-square cut-off for the
            first step.
        n_blocks (int): Number of jackknife blocks.

    Returns:
        dict[str, Any]: Dictionary with keys:
            "h2", "h2_se", "intercept", "intercept_se",
            "slope", "slope_se", "mean_chisq", "lambda_gc",
            "coef", "coef_se".
    """
    beta = np.asarray(beta, dtype=float)
    se = np.asarray(se, dtype=float)
    N = np.asarray(N, dtype=float)
    ld = np.asarray(ld, dtype=float)
    w_ld = np.asarray(w_ld, dtype=float)

    if not (beta.ndim == se.ndim == N.ndim == ld.ndim == w_ld.ndim == 1):
        raise ValueError("All inputs beta, se, N, ld, w_ld must be 1D arrays.")

    Z = beta / se
    chisq = Z**2

    n = chisq.shape[0]
    y = chisq.reshape((n, 1))
    x = ld.reshape((n, 1))
    w = w_ld.reshape((n, 1))
    N_mat = N.reshape((n, 1))
    M_mat = np.array([[float(M_ldsc_scalar)]])

    n_annot = x.shape[1]
    old_weights = False
    if n_annot == 1:
        step_cutoff = twostep if intercept is None else None
    else:
        old_weights = True
        step_cutoff = None

    hsqhat = Hsq(
        y,
        x,
        w,
        N_mat,
        M_mat,
        n_blocks=n_blocks,
        intercept=intercept,
        slow=False,
        twostep=step_cutoff,
        old_weights=old_weights,
    )

    out: dict[str, Any] = {
        "h2": float(hsqhat.tot),
        "h2_se": float(hsqhat.tot_se),
        "intercept": _as_float_or_none(hsqhat.intercept),
        "intercept_se": float(hsqhat.intercept_se),
        "slope": float(hsqhat.coef[0]),
        "slope_se": float(hsqhat.coef_se[0]),
        "mean_chisq": float(hsqhat.mean_chisq),
        "lambda_gc": float(hsqhat.lambda_gc),
        "coef": np.array(hsqhat.coef),
        "coef_se": np.array(hsqhat.coef_se),
    }
    return out
