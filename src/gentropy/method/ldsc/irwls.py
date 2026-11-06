"""Iteratively re-weighted least squares for LDSC."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import numpy as np

from gentropy.method.ldsc.jackknife import LstsqJackknifeFast, _check_shape


class IRWLS:
    """Iteratively re-weighted least squares with block jackknife.

    This class runs a fixed number of weight-update iterations (two by default)
    and then computes a block jackknife covariance for the final weighted
    linear regression.
    """

    def __init__(
        self,
        x: np.ndarray,
        y: np.ndarray,
        update_func: Callable[[tuple[Any, ...]], np.ndarray],
        n_blocks: int,
        w: np.ndarray | None = None,
        slow: bool = False,
        separators: Sequence[int] | None = None,
    ) -> None:
        """Initialise and run IRWLS.

        Args:
            x (np.ndarray): Design matrix of shape (n, p).
            y (np.ndarray): Response vector of shape (n, 1).
            update_func (Callable[[tuple[Any, ...]], np.ndarray]): Callable taking the output of
                `np.linalg.lstsq` and returning new weights of shape (n, 1).
            n_blocks (int): Number of jackknife blocks.
            w (np.ndarray | None): Initial weights of shape (n, 1). If None,
                all weights are set to 1.
            slow (bool): Kept for API compatibility. Ignored (only fast
                jackknife is used).
            separators (Sequence[int] | None): Optional explicit block
                boundaries. If None, blocks are evenly spaced.

        Attributes:
            est (np.ndarray): Least-squares estimate of shape (1, p).
            jknife_se (np.ndarray): Jackknife standard errors of shape (1, p).
            jknife_est (np.ndarray): Jackknife estimates of shape (1, p).
            jknife_var (np.ndarray): Jackknife variances of shape (1, p).
            jknife_cov (np.ndarray): Jackknife covariance matrix of shape
                (p, p).
            delete_values (np.ndarray): Delete values of shape (n_blocks, p).
            separators (np.ndarray): Block boundaries used by the jackknife.

        Raises:
            ValueError: If shapes of `x`, `y` or `w` are incompatible.
        """
        n, p = _check_shape(x, y)
        if w is None:
            w = np.ones_like(y)
        if w.shape != (n, 1):
            raise ValueError(f"w has shape {w.shape}. w must have shape ({n}, 1).")

        jknife = self.irwls(x, y, update_func, n_blocks, w, separators=separators)
        self.est = jknife.est
        self.jknife_se = jknife.jknife_se
        self.jknife_est = jknife.jknife_est
        self.jknife_var = jknife.jknife_var
        self.jknife_cov = jknife.jknife_cov
        self.delete_values = jknife.delete_values
        self.separators = jknife.separators

    @classmethod
    def irwls(
        cls,
        x: np.ndarray,
        y: np.ndarray,
        update_func: Callable[[tuple[Any, ...]], np.ndarray],
        n_blocks: int,
        w: np.ndarray,
        separators: Sequence[int] | None = None,
    ) -> LstsqJackknifeFast:
        """Run the core IRWLS update loop and return a jackknife object.

        This performs two iterations of weight updates:
        1. Compute weighted least squares.
        2. Call `update_func` on the least-squares output to obtain new
           weights.
        3. Repeat once, then fit the final weighted regression and compute
           jackknife statistics.

        Args:
            x (np.ndarray): Design matrix of shape (n, p).
            y (np.ndarray): Response vector of shape (n, 1).
            update_func (Callable[[tuple[Any, ...]], np.ndarray]): Callable taking the output of
                `np.linalg.lstsq` and returning new weights of shape (n, 1).
            n_blocks (int): Number of jackknife blocks.
            w (np.ndarray): Initial weights of shape (n, 1).
            separators (Sequence[int] | None): Optional explicit block
                boundaries.

        Returns:
            LstsqJackknifeFast: Jackknife object containing estimates and
            covariances.

        Raises:
            ValueError: If `y` or `w` shapes are incompatible with `x`, or if
                `update_func` returns weights with mismatched shape.
        """
        n, p = x.shape
        if y.shape != (n, 1):
            raise ValueError(f"y has shape {y.shape}. y must have shape ({n}, 1).")
        if w.shape != (n, 1):
            raise ValueError(f"w has shape {w.shape}. w must have shape ({n}, 1).")

        w = np.sqrt(w)
        for _ in range(2):
            new_w = np.sqrt(update_func(cls.wls(x, y, w)))
            if new_w.shape != w.shape:
                raise ValueError("New weights must have same shape as w.")
            w = new_w

        x_w = cls._weight(x, w)
        y_w = cls._weight(y, w)
        jknife = LstsqJackknifeFast(x_w, y_w, n_blocks, separators=separators)
        return jknife

    @classmethod
    def wls(cls, x: np.ndarray, y: np.ndarray, w: np.ndarray) -> tuple[Any, ...]:
        """Compute weighted least squares using `np.linalg.lstsq`.

        Args:
            x (np.ndarray): Design matrix of shape (n, p).
            y (np.ndarray): Response vector of shape (n, 1).
            w (np.ndarray): Weights of shape (n, 1) on 1/CVF scale.

        Returns:
            tuple[Any, ...]: The output of `np.linalg.lstsq(x_w, y_w, rcond=-1)`, where
            `x_w` and `y_w` are weight-normalised versions of `x` and `y`.

        Raises:
            ValueError: If `y` or `w` shapes are incompatible with `x`.
        """
        n, p = x.shape
        if y.shape != (n, 1):
            raise ValueError(f"y has shape {y.shape}. y must have shape ({n}, 1).")
        if w.shape != (n, 1):
            raise ValueError(f"w has shape {w.shape}. w must have shape ({n}, 1).")

        x_w = cls._weight(x, w)
        y_w = cls._weight(y, w)
        coef = np.linalg.lstsq(x_w, y_w, rcond=-1)
        return coef

    @classmethod
    def _weight(cls, x: np.ndarray, w: np.ndarray) -> np.ndarray:
        """Apply weights to each row of `x` and normalise weights to sum to 1.

        Args:
            x (np.ndarray): Input matrix of shape (n, p).
            w (np.ndarray): Weights of shape (n, 1) on 1/sqrt(CVF) scale.

        Returns:
            np.ndarray: Weighted matrix of shape (n, p).

        Raises:
            ValueError: If any weight is non-positive or if shape of `w` is
                incompatible with `x`.
        """
        if np.any(w <= 0):
            raise ValueError("Weights must be > 0")
        n, p = x.shape
        if w.shape != (n, 1):
            raise ValueError(f"w has shape {w.shape}. w must have shape (n, 1).")
        w_n = w / float(np.sum(w))
        return np.multiply(x, w_n)
