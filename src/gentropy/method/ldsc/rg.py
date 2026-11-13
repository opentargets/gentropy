"""LDSC-style genetic correlation estimation on arrays."""
from __future__ import annotations

from typing import Any

import numpy as np

from gentropy.method.ldsc.jackknife import Jackknife
from gentropy.method.ldsc.regression import GeneticCov, Hsq
from gentropy.method.ldsc.utils import _as_float_or_none


def run_ldsc_rg_from_arrays(
    z1: np.ndarray,
    N1: np.ndarray,
    z2: np.ndarray,
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
    3. Computing rg = gcov / sqrt(h2_1 * h2_2), with its standard error from a
       block jackknife over the same blocks used by the h2 and gcov
       regressions, following Bulik-Sullivan et al. (2015) "An atlas of
       genetic correlations across human diseases and traits" (Nat. Genet.)
       and the `RG` class in the reference implementation
       (https://github.com/bulik/ldsc/blob/master/ldscore/regressions.py).

    Callers are expected to have already reduced effect sizes to Z-scores
    (beta / se) — this is the only quantity used downstream, so passing the
    two components separately would only double the arrays held in memory.

    Args:
        z1 (np.ndarray): Z-scores for trait 1, shape (n_snp,).
        N1 (np.ndarray): Sample sizes for trait 1, shape (n_snp,).
        z2 (np.ndarray): Z-scores for trait 2, shape (n_snp,).
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

    Raises:
        ValueError: If the input arrays are not all 1D with the same shape.
    """
    arrays = (z1, N1, z2, N2, ld, w_ld)
    expected_shape = np.asarray(z1).shape
    if len(expected_shape) != 1 or any(
        np.asarray(arr).shape != expected_shape for arr in arrays
    ):
        raise ValueError("All input arrays must have the same 1D shape.")

    z1 = np.asarray(z1, dtype=float).reshape((-1, 1))
    N1 = np.asarray(N1, dtype=float).reshape((-1, 1))
    z2 = np.asarray(z2, dtype=float).reshape((-1, 1))
    N2 = np.asarray(N2, dtype=float).reshape((-1, 1))
    x = np.asarray(ld, dtype=float).reshape((-1, 1))
    w = np.asarray(w_ld, dtype=float).reshape((-1, 1))
    n = expected_shape[0]

    M_mat = np.array([[float(M_ldsc_scalar)]])
    step_cutoff = twostep if intercept is None else None

    # Step 1: Estimate h2 for each trait
    def _run_hsq(z: np.ndarray, N: np.ndarray) -> Hsq:
        """Run Hsq regression on a single trait's Z-scores.

        Args:
            z (np.ndarray): Z-scores for the trait, shape (n_snp, 1).
            N (np.ndarray): Sample sizes, shape (n_snp, 1).

        Returns:
            Hsq: Fitted heritability regression.
        """
        return Hsq(
            z ** 2, x, w, N, M_mat,
            n_blocks=n_blocks,
            intercept=intercept,
            twostep=step_cutoff,
        )

    hsq1 = _run_hsq(z1, N1)
    hsq2 = _run_hsq(z2, N2)
    h2_1 = float(max(hsq1.tot, 0.0))
    h2_1_se = float(hsq1.tot_se)
    h2_2 = float(max(hsq2.tot, 0.0))
    h2_2_se = float(hsq2.tot_se)

    # Step 2: Genetic covariance regression
    y_gc = z1 * z2

    gencov = GeneticCov(
        y_gc, x, w, N1, N2, M_mat,
        n_blocks=n_blocks,
        intercept=intercept,
        hsq1=h2_1,
        hsq2=h2_2,
        twostep=step_cutoff,
    )

    gcov = float(gencov.tot)
    gcov_se = float(gencov.tot_se)
    intercept_out = _as_float_or_none(gencov.intercept)
    intercept_se_out = gencov.intercept_se

    # Step 3: rg and its block-jackknife SE
    denom_sq = h2_1 * h2_2
    if denom_sq > 0:
        rg_raw = gcov / np.sqrt(denom_sq)
        rg = float(np.clip(rg_raw, -1.0, 1.0))
        rg_clipped = bool(abs(rg_raw) > 1.0)
        rg_se = _rg_jackknife_se(
            rg_raw,
            gencov.tot_delete_values,
            hsq1.tot_delete_values,
            hsq2.tot_delete_values,
        )
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


def _rg_jackknife_se(
    rg: float,
    gcov_delete_values: np.ndarray,
    h2_1_delete_values: np.ndarray,
    h2_2_delete_values: np.ndarray,
) -> float:
    """Estimate the standard error of rg via block jackknife.

    Recomputes rg once per leave-one-block-out delete value of gcov, h2_1,
    and h2_2 (all fit over the same jackknife blocks), converts the
    resulting delete values of rg to pseudovalues, and takes their jackknife
    standard error. This automatically accounts for the covariance between
    the gcov and h2 estimates, unlike a delta-method approximation that
    treats them as independent.

    Args:
        rg (float): Full-sample genetic correlation estimate (unclipped).
        gcov_delete_values (np.ndarray): Per-block delete values of gcov, shape (n_blocks,).
        h2_1_delete_values (np.ndarray): Per-block delete values of h2 for trait 1, shape (n_blocks,).
        h2_2_delete_values (np.ndarray): Per-block delete values of h2 for trait 2, shape (n_blocks,).

    Returns:
        float: Jackknife standard error of rg, or NaN if any block has a
            non-positive h2_1 * h2_2 product.
    """
    denom_sq_delete = h2_1_delete_values * h2_2_delete_values
    if np.any(denom_sq_delete <= 0):
        return float("nan")

    rg_delete_values = gcov_delete_values / np.sqrt(denom_sq_delete)
    pseudovalues = Jackknife.delete_values_to_pseudovalues(
        rg_delete_values.reshape((-1, 1)), np.array([[rg]])
    )
    _, _, jknife_se, _ = Jackknife.jknife(pseudovalues)
    return float(jknife_se[0, 0])
