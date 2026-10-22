"""Utility helpers for LDSC regression."""
from __future__ import annotations

from typing import Any

import numpy as np


def append_intercept(x: np.ndarray) -> np.ndarray:
    """Append a column of ones as an intercept term to the design matrix.

    Args:
        x (np.ndarray): Array of shape (n_row, n_col).

    Returns:
        np.ndarray: Augmented array of shape (n_row, n_col + 1) with a
        trailing intercept column of ones.
    """
    n_row = x.shape[0]
    intercept = np.ones((n_row, 1))
    return np.concatenate((x, intercept), axis=1)


def update_separators(s: np.ndarray, ii: np.ndarray) -> np.ndarray:
    """Map separators from a masked array back to unmasked indices.

    This is used in two-step LDSC, where the first step is run on a subset of
    SNPs (indexed by `ii`) and the second step reuses the same block structure
    on the full data.

    Args:
        s (np.ndarray): Block boundaries for the masked subset, of length
            n_blocks + 1.
        ii (np.ndarray): Boolean mask of length n_snp indicating which SNPs
            were kept in step 1.

    Returns:
        np.ndarray: Block boundaries for the full unmasked set of SNPs, of
        length n_blocks + 1.
    """
    maplist = np.arange(len(ii))[np.squeeze(ii)]
    mask_to_unmask = lambda i: maplist[i]
    t = np.apply_along_axis(mask_to_unmask, 0, s[1:-1])
    t = np.hstack((0, t, len(ii)))
    return t


def _as_float_or_none(x: Any) -> float | None:
    """Convert a value to float, returning None if the input is None.

    Args:
        x (Any): Value to convert.

    Returns:
        float | None: Float representation of x, or None.
    """
    return None if x is None else float(x)
