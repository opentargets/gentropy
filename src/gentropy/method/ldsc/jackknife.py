"""Block jackknife utilities for LDSC regression."""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np

np.seterr(divide="raise", invalid="raise")


def _check_shape(x: np.ndarray, y: np.ndarray) -> tuple[int, int]:
    """Check that arrays have compatible 2D shapes for regression jackknives.

    Args:
        x (np.ndarray): Design matrix of shape (n, p).
        y (np.ndarray): Response vector of shape (n, 1).

    Returns:
        tuple[int, int]: The pair (n, p) where n is the number of rows and p
        is the number of columns in `x`.

    Raises:
        ValueError: If `x` or `y` is not 2D, if their row counts differ, if
            `y` does not have shape (n, 1), or if `p > n`.
    """
    if len(x.shape) != 2 or len(y.shape) != 2:
        raise ValueError("x and y must be 2D arrays.")
    if x.shape[0] != y.shape[0]:
        raise ValueError("Number of datapoints in x != number of datapoints in y.")
    if y.shape[1] != 1:
        raise ValueError("y must have shape (n_snp, 1)")
    n, p = x.shape
    if p > n:
        raise ValueError("More dimensions than datapoints.")
    return n, p


def _check_shape_block(
    xty_block_values: np.ndarray, xtx_block_values: np.ndarray
) -> tuple[int, int]:
    """Check that blockwise X'Y and X'X arrays have compatible shapes.

    Args:
        xty_block_values (np.ndarray): Block values for X'Y with shape
            (n_blocks, p).
        xtx_block_values (np.ndarray): Block values for X'X with shape
            (n_blocks, p, p).

    Returns:
        tuple[int, int]: The pair (n_blocks, p).

    Raises:
        ValueError: If shapes are inconsistent, if `xtx_block_values` is not
            3D, or if its last two axes do not have equal size.
    """
    if xtx_block_values.shape[0:2] != xty_block_values.shape:
        raise ValueError(
            "Shape of xty_block_values must equal first two dims of xtx_block_values."
        )
    if len(xtx_block_values.shape) < 3:
        raise ValueError("xtx_block_values must be a 3D array.")
    if xtx_block_values.shape[1] != xtx_block_values.shape[2]:
        raise ValueError("Last two axes of xtx_block_values must have same dimension.")
    return xtx_block_values.shape[0:2]


class Jackknife:
    """Base class for block jackknife estimators.

    This class assumes statistics derived from independent variables `x` and
    dependent variables `y` (for example linear regression estimates). It
    provides helpers to construct jackknife pseudovalues and estimates from
    per-block delete values.

    Attributes:
        N (int): Number of datapoints (rows in `x` and `y`).
        p (int): Dimensionality of the parameter vector.
        n_blocks (int): Number of jackknife blocks.
        separators (np.ndarray): Block boundaries as integer indices of
            length `n_blocks + 1`.
    """

    def __init__(
        self,
        x: np.ndarray,
        y: np.ndarray,
        n_blocks: int | None = None,
        separators: Sequence[int] | None = None,
    ) -> None:
        """Initialise a Jackknife object.

        One of `n_blocks` or `separators` must be provided.

        Args:
            x (np.ndarray): Design matrix of shape (n, p).
            y (np.ndarray): Response vector of shape (n, 1).
            n_blocks (int | None): Number of jackknife blocks. If provided,
                evenly spaced block boundaries are constructed.
            separators (Sequence[int] | None): Monotonically increasing
                integer indices (including 0 and n) that define block
                boundaries.

        Raises:
            ValueError: If neither `n_blocks` nor `separators` is given, if
                separators do not start at 0 and end at n, or if
                `n_blocks > n`.
            TypeError: If `x` or `y` is not array-like with 2 dimensions.
        """
        self.N, self.p = _check_shape(x, y)
        if separators is not None:
            if max(separators) != self.N:
                raise ValueError(
                    "Max(separators) must be equal to number of data points."
                )
            if min(separators) != 0:
                raise ValueError("Min(separators) must be equal to 0.")
            self.separators = np.array(sorted(separators), dtype=int)
            self.n_blocks = len(separators) - 1
        elif n_blocks is not None:
            self.n_blocks = n_blocks
            self.separators = self.get_separators(self.N, self.n_blocks)
        else:
            raise ValueError("Must specify either n_blocks or separators.")

        if self.n_blocks > self.N:
            raise ValueError("More blocks than data points.")

    @classmethod
    def jknife(
        cls, pseudovalues: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Convert jackknife pseudovalues to estimate, variance and covariance.

        Args:
            pseudovalues (np.ndarray): Jackknife pseudovalues of shape
                (n_blocks, p).

        Returns:
            tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
                jknife_est, jknife_var, jknife_se, jknife_cov.
        """
        n_blocks = pseudovalues.shape[0]
        jknife_cov = np.atleast_2d(np.cov(pseudovalues.T, ddof=1) / n_blocks)
        jknife_var = np.atleast_2d(np.diag(jknife_cov))
        jknife_se = np.atleast_2d(np.sqrt(jknife_var))
        jknife_est = np.atleast_2d(np.mean(pseudovalues, axis=0))
        return jknife_est, jknife_var, jknife_se, jknife_cov

    @classmethod
    def delete_values_to_pseudovalues(
        cls, delete_values: np.ndarray, est: np.ndarray
    ) -> np.ndarray:
        """Convert delete-values and full-data estimate to jackknife pseudovalues.

        Args:
            delete_values (np.ndarray): Delete values of shape (n_blocks, p).
            est (np.ndarray): Whole-data estimate of shape (1, p).

        Returns:
            np.ndarray: Jackknife pseudovalues of shape (n_blocks, p).

        Raises:
            ValueError: If `est` shape is incompatible with `delete_values`.
        """
        n_blocks, p = delete_values.shape
        if est.shape != (1, p):
            raise ValueError(
                "Different number of parameters in delete_values than in est."
            )
        return n_blocks * est - (n_blocks - 1) * delete_values

    @classmethod
    def get_separators(cls, N: int, n_blocks: int) -> np.ndarray:
        """Construct evenly spaced jackknife block boundaries.

        Args:
            N (int): Number of datapoints.
            n_blocks (int): Number of blocks to create.

        Returns:
            np.ndarray: Integer array of length `n_blocks + 1` with block
            boundaries from 0 to N inclusive.
        """
        return np.floor(np.linspace(0, N, n_blocks + 1)).astype(int)


class LstsqJackknifeFast(Jackknife):
    """Fast block jackknife for linear regression.

    Uses block-wise sums of X'Y and X'X to compute delete values and the
    jackknife covariance in one pass, without refitting the regression for
    each block.
    """

    def __init__(
        self,
        x: np.ndarray,
        y: np.ndarray,
        n_blocks: int | None = None,
        separators: Sequence[int] | None = None,
    ) -> None:
        """Initialise the fast jackknife for linear regression.

        Args:
            x (np.ndarray): Design matrix of shape (n, p).
            y (np.ndarray): Response vector of shape (n, 1).
            n_blocks (int | None): Number of jackknife blocks.
            separators (Sequence[int] | None): Explicit block boundaries.

        Raises:
            ValueError: If block configuration is invalid.
            ValueError: If linear solves fail due to singular X'X for a block.
        """
        super().__init__(x, y, n_blocks, separators)
        xty, xtx = self.block_values(x, y, self.separators)
        self.est = self.block_values_to_est(xty, xtx)
        self.delete_values = self.block_values_to_delete_values(xty, xtx)
        self.pseudovalues = self.delete_values_to_pseudovalues(
            self.delete_values, self.est
        )
        (
            self.jknife_est,
            self.jknife_var,
            self.jknife_se,
            self.jknife_cov,
        ) = self.jknife(self.pseudovalues)

    @classmethod
    def block_values(
        cls, x: np.ndarray, y: np.ndarray, s: Sequence[int]
    ) -> tuple[np.ndarray, np.ndarray]:
        """Compute block-wise X'Y and X'X for linear regression.

        Args:
            x (np.ndarray): Design matrix of shape (n, p).
            y (np.ndarray): Response vector of shape (n, 1).
            s (Sequence[int]): Block boundaries (length n_blocks + 1).

        Returns:
            tuple[np.ndarray, np.ndarray]:
                xty_block_values, xtx_block_values.
        """
        n, p = _check_shape(x, y)
        n_blocks = len(s) - 1
        xtx_block_values = np.zeros((n_blocks, p, p))
        xty_block_values = np.zeros((n_blocks, p))
        for i in range(n_blocks):
            xs = x[s[i] : s[i + 1], ...]
            ys = y[s[i] : s[i + 1], ...]
            xty_block_values[i, ...] = np.dot(xs.T, ys).reshape((1, p))
            xtx_block_values[i, ...] = np.dot(xs.T, xs)
        return xty_block_values, xtx_block_values

    @classmethod
    def block_values_to_est(
        cls, xty_block_values: np.ndarray, xtx_block_values: np.ndarray
    ) -> np.ndarray:
        """Convert block-wise sums to full-data least-squares estimate.

        Args:
            xty_block_values (np.ndarray): Block X'Y values of shape
                (n_blocks, p).
            xtx_block_values (np.ndarray): Block X'X values of shape
                (n_blocks, p, p).

        Returns:
            np.ndarray: Least-squares estimate of shape (1, p).
        """
        n_blocks, p = _check_shape_block(xty_block_values, xtx_block_values)
        xty = np.sum(xty_block_values, axis=0)
        xtx = np.sum(xtx_block_values, axis=0)
        return np.linalg.solve(xtx, xty).reshape((1, p))

    @classmethod
    def block_values_to_delete_values(
        cls, xty_block_values: np.ndarray, xtx_block_values: np.ndarray
    ) -> np.ndarray:
        """Convert block-wise sums to delete values.

        For each jackknife block j, this recomputes the least-squares estimate
        using all blocks except j.

        Args:
            xty_block_values (np.ndarray): Block X'Y values of shape
                (n_blocks, p).
            xtx_block_values (np.ndarray): Block X'X values of shape
                (n_blocks, p, p).

        Returns:
            np.ndarray: Delete values of shape (n_blocks, p).
        """
        n_blocks, p = _check_shape_block(xty_block_values, xtx_block_values)
        delete_values = np.zeros((n_blocks, p))
        xty_tot = np.sum(xty_block_values, axis=0)
        xtx_tot = np.sum(xtx_block_values, axis=0)
        for j in range(n_blocks):
            delete_xty = xty_tot - xty_block_values[j]
            delete_xtx = xtx_tot - xtx_block_values[j]
            delete_values[j, ...] = np.linalg.solve(delete_xtx, delete_xty).reshape(
                (1, p)
            )
        return delete_values
