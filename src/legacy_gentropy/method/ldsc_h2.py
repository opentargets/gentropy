"""Minimal self-contained LDSC-style SNP-heritability estimation on arrays.

Ported and adapted from the original LDSC code. Implements:

- Block jackknife utilities for linear regression.
- Iteratively re-weighted least squares (IRWLS).
- LDSC-style SNP-heritability regression for a single trait.
- A convenience wrapper that runs LDSC on plain NumPy arrays.
"""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable, Sequence
from typing import Any

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
    return None if x is None else float(x)
class LD_Score_Regression:
    """Base class for LD Score regression (heritability and genetic covariance).

    This implements the core linear regression and jackknife logic that is
    specialised by subclasses such as `Hsq` for SNP-heritability.

    Attributes:
        n_annot (int): Number of LD-score annotations (columns in `x`).
        constrain_intercept (bool): Whether the intercept is fixed to a
            specified value.
        intercept (float | np.ndarray | None): Intercept value or estimate.
        intercept_se (float | str): Intercept standard error or "NA" if fixed.
        coef (np.ndarray): Per-annotation regression coefficients.
        coef_cov (np.ndarray): Covariance of coefficients.
        coef_se (np.ndarray): Standard errors of coefficients.
        tot (float): Total heritability or covariance (sum over annotations).
        tot_cov (float): Variance of `tot`.
        tot_se (float): Standard error of `tot`.
        jknife (Any): Jackknife object containing delete values and covariances.
        M (np.ndarray): Row vector of annotation sizes used in the regression.
    """

    def __init__(
        self,
        y: np.ndarray,
        x: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M: np.ndarray,
        n_blocks: int,
        intercept: float | None = None,
        slow: bool = False,
        step1_ii: np.ndarray | None = None,
        old_weights: bool = False,
    ) -> None:
        """Initialise LD Score regression with jackknife estimation.

        Args:
            y (np.ndarray): Response vector of shape (n_snp, 1), typically
                chi-square statistics or z1*z2 products.
            x (np.ndarray): LD-score design matrix of shape (n_snp, n_annot).
            w (np.ndarray): Initial weights of shape (n_snp, 1), often LD
                scores computed on the regression SNP set.
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (np.ndarray): Row vector of annotation sizes of shape
                (1, n_annot), usually the total number of SNPs contributing to
                each annotation's LD score.
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Fixed intercept value. If None, the
                intercept is estimated.
            slow (bool): Kept for compatibility. Ignored in this
                implementation (only fast jackknife is used).
            step1_ii (np.ndarray | None): Boolean mask for two-step LDSC. If
                provided and intercept is free, a first-step regression is
                run on this subset before the second step on all SNPs.
            old_weights (bool): If True, use LDSC's original weighting scheme
                instead of the IRWLS updates. Only used for multi-annotation
                regressions.

        Raises:
            TypeError: If any of `y`, `x`, `w`, `M`, `N` is not array-like with
                a `shape` attribute or is not 2D.
            ValueError: If shapes of inputs are incompatible, or if two-step
                LDSC is requested in unsupported settings.
        """
        self._validate_and_init_state(y, x, w, N, M, n_blocks, intercept)

        (
            x,
            x_tot,
            yp,
            Nbar,
            M_tot,
            initial_w,
        ) = self._prepare_design(y, x, w, N, M, intercept)

        self._check_twostep_compatibility(step1_ii)

        jknife = self._fit_jackknife(
            x=x,
            x_tot=x_tot,
            yp=yp,
            w=w,
            N=N,
            M_tot=M_tot,
            Nbar=Nbar,
            n_blocks=n_blocks,
            intercept=intercept,
            slow=slow,
            step1_ii=step1_ii,
            old_weights=old_weights,
            initial_w=initial_w,
        )

        self._extract_results(jknife, M, Nbar)

    def _validate_and_init_state(
        self,
        y: np.ndarray,
        x: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M: np.ndarray,
        n_blocks: int,
        intercept: float | None,
    ) -> None:
        """Validate array inputs and initialise basic attributes.

        Args:
            y (np.ndarray): Response vector of shape (n_snp, 1).
            x (np.ndarray): LD-score design matrix of shape (n_snp, n_annot).
            w (np.ndarray): Regression weights of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (np.ndarray): Row vector of annotation sizes of shape
                (1, n_annot).
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Fixed intercept value, if any.

        Raises:
            TypeError: If any argument is not array-like with 2 dimensions.
            ValueError: If shapes are incompatible.
        """
        for i in [y, x, w, M, N]:
            try:
                if len(i.shape) != 2:
                    raise TypeError("Arguments must be 2D arrays.")
            except AttributeError as err:
                raise TypeError("Arguments must be arrays.") from err

        n_snp, self.n_annot = x.shape
        if any(i.shape != (n_snp, 1) for i in (y, w, N)):
            raise ValueError("N, weights and response must have shape (n_snp, 1).")
        if M.shape != (1, self.n_annot):
            raise ValueError("M must have shape (1, n_annot).")

        self.constrain_intercept: bool = intercept is not None
        self.intercept: float | None = intercept
        self.n_blocks: int = n_blocks
        self.twostep_filtered: int | None = None

    def _prepare_design(
        self,
        y: np.ndarray,
        x: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M: np.ndarray,
        intercept: float | None,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float, np.ndarray]:
        """Compute initial weights, scale design matrix, and handle intercept.

        Args:
            y (np.ndarray): Response vector of shape (n_snp, 1).
            x (np.ndarray): LD-score design matrix of shape (n_snp, n_annot).
            w (np.ndarray): Regression weights of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (np.ndarray): Row vector of annotation sizes of shape
                (1, n_annot).
            intercept (float | None): Fixed intercept value, if any. If None,
                the intercept is estimated.

        Returns:
            tuple[np.ndarray, np.ndarray, np.ndarray, float, float, np.ndarray]:
                Tuple containing:
                    x (np.ndarray): Rescaled design matrix.
                    x_tot (np.ndarray): Total LD scores per SNP.
                    yp (np.ndarray): Response with intercept handled.
                    Nbar (float): Mean sample size.
                    M_tot (float): Total number of SNPs used in LD scores.
                    initial_w (np.ndarray): Initial weights for IRWLS.
        """
        n_snp = x.shape[0]
        M_tot: float = float(np.sum(M))
        x_tot = np.sum(x, axis=1).reshape((n_snp, 1))

        tot_agg: float = self.aggregate(y, x_tot, N, M_tot, intercept)
        initial_w = self._update_weights(x_tot, w, N, M_tot, tot_agg, intercept)  # type: ignore[attr-defined]

        Nbar: float = float(np.mean(N))  # keep condition number low
        x = np.multiply(N, x) / Nbar

        if not self.constrain_intercept:
            x = append_intercept(x)
            x_tot = append_intercept(x_tot)
            yp = y
        else:
            yp = y - intercept
            self.intercept_se = "NA"

        return x, x_tot, yp, Nbar, M_tot, initial_w

    def _check_twostep_compatibility(
        self,
        step1_ii: np.ndarray | None,
    ) -> None:
        """Validate that two-step LDSC is used only in supported settings.

        Args:
            step1_ii (np.ndarray | None): Boolean mask for the first-step
                regression, or None if two-step LDSC is not used.

        Raises:
            ValueError: If two-step LDSC is requested with a constrained
                intercept or with partitioned LD Score (n_annot > 1).
        """
        if step1_ii is None:
            return

        if self.constrain_intercept:
            raise ValueError("twostep is not compatible with constrained intercept.")
        if self.n_annot > 1:
            raise ValueError("twostep not compatible with partitioned LD Score yet.")

    def _fit_jackknife(
        self,
        x: np.ndarray,
        x_tot: np.ndarray,
        yp: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M_tot: float,
        Nbar: float,
        n_blocks: int,
        intercept: float | None,
        slow: bool,
        step1_ii: np.ndarray | None,
        old_weights: bool,
        initial_w: np.ndarray,
    ) -> Any:
        """Dispatch to the appropriate LDSC regression path and fit jackknife.

        Args:
            x (np.ndarray): Design matrix of shape (n_snp, n_annot [+1 if intercept]).
            x_tot (np.ndarray): Total LD scores per SNP, possibly with intercept.
            yp (np.ndarray): Response vector after intercept handling.
            w (np.ndarray): LD-based regression weights of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M_tot (float): Total number of SNPs used for LD scores.
            Nbar (float): Mean sample size.
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Fixed intercept value, if any.
            slow (bool): Kept for compatibility with older interfaces.
            step1_ii (np.ndarray | None): Boolean mask for two-step LDSC.
            old_weights (bool): Use original LDSC weighting scheme if True.
            initial_w (np.ndarray): Initial weights for IRWLS.

        Returns:
            Any: A jackknife object with attributes `est`, `jknife_cov`,
                `jknife_se`, and `delete_values`.
        """
        if step1_ii is not None:
            return self._run_twostep_ldsc(
                x=x,
                x_tot=x_tot,
                yp=yp,
                w=w,
                N=N,
                M_tot=M_tot,
                Nbar=Nbar,
                n_blocks=n_blocks,
                slow=slow,
                step1_ii=step1_ii,
                initial_w=initial_w,
            )

        if old_weights:
            return self._run_old_weights_ldsc(
                x=x,
                yp=yp,
                n_blocks=n_blocks,
                initial_w=initial_w,
            )

        return self._run_irwls_ldsc(
            x=x,
            x_tot=x_tot,
            yp=yp,
            w=w,
            N=N,
            M_tot=M_tot,
            Nbar=Nbar,
            n_blocks=n_blocks,
            slow=slow,
            intercept=intercept,
            initial_w=initial_w,
        )

    def _run_twostep_ldsc(
        self,
        x: np.ndarray,
        x_tot: np.ndarray,
        yp: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M_tot: float,
        Nbar: float,
        n_blocks: int,
        slow: bool,
        step1_ii: np.ndarray,
        initial_w: np.ndarray,
    ) -> Any:
        """Run two-step LDSC (free intercept then constrained intercept).

        Args:
            x (np.ndarray): Design matrix (with intercept column) of shape
                (n_snp, n_annot + 1).
            x_tot (np.ndarray): Total LD scores per SNP (with intercept) of
                shape (n_snp, 1 or 2).
            yp (np.ndarray): Response vector after initial intercept handling.
            w (np.ndarray): LD-based regression weights of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M_tot (float): Total number of SNPs used for LD scores.
            Nbar (float): Mean sample size.
            n_blocks (int): Number of jackknife blocks.
            slow (bool): Kept for compatibility; passed through to IRWLS.
            step1_ii (np.ndarray): Boolean mask for first-step SNP subset.
            initial_w (np.ndarray): Initial weights for IRWLS.

        Returns:
            Any: Jackknife-like namedtuple with fields:
                est, jknife_se, jknife_est, jknife_var, jknife_cov, delete_values.
        """
        n_snp = x.shape[0]
        n1 = int(np.sum(step1_ii))
        self.twostep_filtered = int(n_snp - n1)

        x1 = x[np.squeeze(step1_ii), :]
        yp1, w1, N1, initial_w1 = [
            a[step1_ii].reshape((n1, 1)) for a in (yp, w, N, initial_w)
        ]

        update_func1 = lambda a: self._update_func(
            a,
            x1,
            w1,
            N1,
            M_tot,
            Nbar,
            ii=step1_ii,
        )
        step1_jknife = IRWLS(
            x1,
            yp1,
            update_func1,
            n_blocks,
            slow=slow,
            w=initial_w1,
        )
        step1_int, _ = self._intercept(step1_jknife)

        yp2 = yp - step1_int
        x2 = x[:, :-1]
        x_tot2 = x_tot[:, :-1]

        update_func2 = lambda a: self._update_func(
            a,
            x_tot2,
            w,
            N,
            M_tot,
            Nbar,
            step1_int,
        )
        s = update_separators(step1_jknife.separators, step1_ii)
        step2_jknife = IRWLS(
            x2,
            yp2,
            update_func2,
            n_blocks,
            slow=slow,
            w=initial_w,
            separators=s,
        )
        c: float = float(
            np.sum(np.multiply(initial_w, x2))
            / np.sum(np.multiply(initial_w, np.square(x2)))
        )
        return self._combine_twostep_jknives(step1_jknife, step2_jknife, M_tot, c, Nbar)

    def _run_old_weights_ldsc(
        self,
        x: np.ndarray,
        yp: np.ndarray,
        n_blocks: int,
        initial_w: np.ndarray,
    ) -> Any:
        """Run LDSC using the original weighting scheme (no IRWLS updates).

        Args:
            x (np.ndarray): Design matrix of shape (n_snp, n_annot [+1]).
            yp (np.ndarray): Response vector after intercept handling.
            n_blocks (int): Number of jackknife blocks.
            initial_w (np.ndarray): Initial weights for the regression.

        Returns:
            Any: Jackknife object from `LstsqJackknifeFast`.
        """
        w0 = np.sqrt(initial_w)
        x_w = IRWLS._weight(x, w0)
        y_w = IRWLS._weight(yp, w0)
        return LstsqJackknifeFast(x_w, y_w, n_blocks)

    def _run_irwls_ldsc(
        self,
        x: np.ndarray,
        x_tot: np.ndarray,
        yp: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M_tot: float,
        Nbar: float,
        n_blocks: int,
        slow: bool,
        intercept: float | None,
        initial_w: np.ndarray,
    ) -> Any:
        """Run the default IRWLS-based LDSC regression.

        Args:
            x (np.ndarray): Design matrix of shape (n_snp, n_annot [+1]).
            x_tot (np.ndarray): Total LD scores per SNP, possibly with intercept.
            yp (np.ndarray): Response vector after intercept handling.
            w (np.ndarray): LD-based regression weights of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M_tot (float): Total number of SNPs used for LD scores.
            Nbar (float): Mean sample size.
            n_blocks (int): Number of jackknife blocks.
            slow (bool): Kept for compatibility; passed through to IRWLS.
            intercept (float | None): Intercept value used in weight updates.
            initial_w (np.ndarray): Initial weights for IRWLS.

        Returns:
            Any: Jackknife object from `IRWLS`.
        """
        update_func = lambda a: self._update_func(
            a,
            x_tot,
            w,
            N,
            M_tot,
            Nbar,
            intercept,
        )
        return IRWLS(
            x,
            yp,
            update_func,
            n_blocks,
            slow=slow,
            w=initial_w,
        )

    def _extract_results(
        self,
        jknife: Any,
        M: np.ndarray,
        Nbar: float,
    ) -> None:
        """Populate regression and summary attributes from jackknife results.

        Args:
            jknife (Any): Jackknife object with fields `est`, `jknife_cov`,
                and `jknife_se`.
            M (np.ndarray): Row vector of annotation sizes of shape
                (1, n_annot).
            Nbar (float): Mean sample size used when rescaling coefficients.
        """
        self.coef, self.coef_cov, self.coef_se = self._coef(jknife, Nbar)
        self.tot, self.tot_cov, self.tot_se = self._tot_from_coef(
            M,
            self.coef,
            self.coef_cov,
        )

        if not self.constrain_intercept:
            self.intercept, self.intercept_se = self._intercept(jknife)

        self.jknife = jknife
        self.M = M


    @classmethod
    def aggregate(
        cls,
        y: np.ndarray,
        x: np.ndarray,
        N: np.ndarray,
        M: float,
        intercept: float | None = None,
    ) -> float:
        """Initial aggregate estimate used to set starting weights.

        This matches the approximate LDSC formula:

            E[y] ≈ intercept + (N / M) * x * h2

        Args:
            y (np.ndarray): Response vector of shape (n_snp, 1).
            x (np.ndarray): Total LD scores per SNP of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (float): Total number of SNPs used in LD-score estimation.
            intercept (float | None): Intercept value. If None, uses
                `cls.__null_intercept__`.

        Returns:
            float: Initial aggregate heritability or covariance estimate.
        """
        if intercept is None:
            intercept = cls.__null_intercept__  # type: ignore[attr-defined]
        num = M * (np.mean(y) - intercept)
        denom = np.mean(np.multiply(x, N))
        return float(num / denom)

    def _update_func(
        self,
        x: tuple[Any, ...],
        ref_ld_tot: np.ndarray,
        w_ld: np.ndarray,
        N: np.ndarray,
        M: float,
        Nbar: float,
        intercept: float | None = None,
        ii: np.ndarray | None = None,
    ) -> np.ndarray:
        """Update function for IRWLS.

        This must be implemented by subclasses such as `Hsq`.

        Args:
            x (tuple[Any, ...]): Output of `np.linalg.lstsq`.
            ref_ld_tot (np.ndarray): Reference LD design matrix used to
                construct weights.
            w_ld (np.ndarray): LD-based weighting LD scores of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (float): Total number of SNPs used for LD scores.
            Nbar (float): Mean sample size.
            intercept (float | None): Intercept used when building weights.
            ii (np.ndarray | None): Optional mask for subsetting N and LD.

        Returns:
            np.ndarray: New weights of shape (n_snp, 1).
        """
        raise NotImplementedError

    def _coef(
        self,
        jknife: Any,
        Nbar: float,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Extract regression coefficients and covariance from jackknife.

        Args:
            jknife (Any): Jackknife object with `est` and `jknife_cov` attributes.
            Nbar (float): Mean sample size used to rescale coefficients.

        Returns:
            tuple[np.ndarray, np.ndarray, np.ndarray]:
                coef, coef_cov, coef_se.
        """
        n_annot = self.n_annot
        coef = jknife.est[0, 0:n_annot] / Nbar
        coef_cov = jknife.jknife_cov[0:n_annot, 0:n_annot] / Nbar**2
        coef_se = np.sqrt(np.diag(coef_cov))
        return coef, coef_cov, coef_se

    def _tot_from_coef(
        self, M: np.ndarray, coef: np.ndarray, coef_cov: np.ndarray
    ) -> tuple[float, float, float]:
        """Convert per-annotation coefficients to total h2 or covariance.

        Args:
            M (np.ndarray): Row vector of annotation sizes of shape
                (1, n_annot).
            coef (np.ndarray): Per-annotation coefficients of shape
                (n_annot,).
            coef_cov (np.ndarray): Covariance of coefficients of shape
                (n_annot, n_annot).

        Returns:
            tuple[float, float, float]:
                tot, tot_cov, tot_se.
        """
        M_vec = M.reshape(-1)
        tot = float(M_vec @ coef)
        tot_cov = float(M_vec @ coef_cov @ M_vec)
        tot_se = float(np.sqrt(max(tot_cov, 0.0)))
        return tot, tot_cov, tot_se

    def _intercept(self, jknife: Any) -> tuple[Any, ...]:
        """Extract intercept and its standard error from jackknife.

        Args:
            jknife (Any): Jackknife object with `est` and `jknife_se` attributes.

        Returns:
            tuple[Any, ...]: Intercept and its standard error.
        """
        n_annot = self.n_annot
        intercept = float(jknife.est[0, n_annot])
        intercept_se = float(jknife.jknife_se[0, n_annot])
        return intercept, intercept_se

    def _combine_twostep_jknives(
        self,
        step1_jknife: Any,
        step2_jknife: Any,
        M_tot: float,
        c: float,
        Nbar: float = 1.0,
    ) -> Any:
        """Combine free and constrained intercept jackknives for two-step LDSC.

        Args:
            step1_jknife (Any): Jackknife object from the first (free-intercept) step.
            step2_jknife (Any): Jackknife object from the second (constrained) step.
            M_tot (float): Total number of SNPs (kept for parity with original LDSC).
            c (float): Scaling constant relating intercept differences to slope differences.
            Nbar (float): Mean sample size (unused here but kept for API consistency).

        Returns:
            Any: A jackknife-like namedtuple with fields:
                est, jknife_se, jknife_est, jknife_var, jknife_cov, delete_values.

        Raises:
            ValueError: If the number of annotations is greater than 2 (not
                implemented for partitioned LD Score).
        """
        n_blocks, n_annot = step1_jknife.delete_values.shape
        n_annot -= 1
        if n_annot > 2:
            raise ValueError("twostep not yet implemented for partitioned LD Score.")

        step1_int, _ = self._intercept(step1_jknife)
        est = np.hstack((step2_jknife.est, np.array(step1_int).reshape((1, 1))))
        delete_values = np.zeros((n_blocks, n_annot + 1))
        delete_values[:, n_annot] = step1_jknife.delete_values[:, n_annot]
        delete_values[:, 0:n_annot] = step2_jknife.delete_values - c * (
            step1_jknife.delete_values[:, n_annot] - step1_int
        ).reshape((n_blocks, n_annot))
        pseudovalues = Jackknife.delete_values_to_pseudovalues(delete_values, est)
        jknife_est, jknife_var, jknife_se, jknife_cov = Jackknife.jknife(pseudovalues)
        Jknife = namedtuple(
            "Jknife",
            [
                "est",
                "jknife_se",
                "jknife_est",
                "jknife_var",
                "jknife_cov",
                "delete_values",
            ],
        )
        return Jknife(est, jknife_se, jknife_est, jknife_var, jknife_cov, delete_values)


class Hsq(LD_Score_Regression):
    """LDSC-style SNP-heritability regression for a single trait.

    This class regresses chi-square statistics (or z-scores squared) on LD
    scores to estimate SNP-heritability, intercept (confounding) and ratio.
    """

    __null_intercept__ = 1.0

    def __init__(
        self,
        y: np.ndarray,
        x: np.ndarray,
        w: np.ndarray,
        N: np.ndarray,
        M: np.ndarray,
        n_blocks: int = 200,
        intercept: float | None = None,
        slow: bool = False,
        twostep: float | None = None,
        old_weights: bool = False,
    ) -> None:
        """Initialise SNP-heritability regression.

        Args:
            y (np.ndarray): Vector of chi-square statistics of shape
                (n_snp, 1).
            x (np.ndarray): LD-score design matrix of shape (n_snp, n_annot).
            w (np.ndarray): LD-based weights of shape (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (np.ndarray): Row vector of annotation sizes of shape
                (1, n_annot).
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Fixed intercept. If None, intercept is
                estimated.
            slow (bool): Kept for compatibility, ignored here.
            twostep (float | None): If not None and `n_annot == 1` and
                intercept is free, run two-step LDSC using this chi-square
                cut-off for step 1.
            old_weights (bool): If True and `n_annot > 1`, use LDSC's
                original weighting scheme without IRWLS updates.

        Attributes:
            mean_chisq (float): Mean chi-square across SNPs.
            lambda_gc (float): Genomic control inflation factor.
            ratio (float | str): LDSC ratio `(intercept - 1)/(mean_chisq - 1)`
                when mean chi-square is greater than 1, else "NA".
            ratio_se (float | str): Standard error of `ratio`, or "NA".
        """
        step1_ii: np.ndarray | None = None
        if twostep is not None:
            step1_ii = y < twostep

        super().__init__(
            y,
            x,
            w,
            N,
            M,
            n_blocks,
            intercept=intercept,
            slow=slow,
            step1_ii=step1_ii,
            old_weights=old_weights,
        )
        self.mean_chisq, self.lambda_gc = self._summarise_chisq(y)
        if not self.constrain_intercept:
            self.ratio, self.ratio_se = self._ratio(
                float(self.intercept), float(self.intercept_se), self.mean_chisq  # type: ignore[arg-type]
            )
        else:
            self.ratio, self.ratio_se = None, None

    def _update_func(
        self,
        x: tuple[Any, ...],
        ref_ld_tot: np.ndarray,
        w_ld: np.ndarray,
        N: np.ndarray,
        M: float,
        Nbar: float,
        intercept: float | None = None,
        ii: np.ndarray | None = None,
    ) -> np.ndarray:
        """Update function for IRWLS used in LDSC heritability regression.

        Args:
            x (tuple[Any, ...]): Output of `np.linalg.lstsq`, where `x[0]` is the
                coefficient vector and the last element is the intercept when free.
            ref_ld_tot (np.ndarray): Total LD design matrix of shape
                (n_snp, 1) or (n_snp, 2) if an intercept column is included.
            w_ld (np.ndarray): LD-based weighting LD scores of shape
                (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (float): Total number of SNPs used for LD scores.
            Nbar (float): Mean sample size.
            intercept (float | None): Fixed intercept value, if any.
            ii (np.ndarray | None): Optional mask for subsetting N and LD.

        Returns:
            np.ndarray: Updated regression weights of shape (n_snp, 1).
        """
        hsq = M * x[0][0] / Nbar
        if intercept is None:
            intercept_eff = float(max(x[0][1]))  # avoids negative intercept in weights
        else:
            intercept_eff = float(intercept)
            if ref_ld_tot.shape[1] > 1:
                raise ValueError(
                    "Design matrix has intercept column for constrained intercept regression."
                )

        ld = ref_ld_tot[:, 0].reshape(w_ld.shape)
        w = self.weights(ld, w_ld, N, M, float(hsq), intercept_eff, ii)

        return w

    def _summarise_chisq(self, chisq: np.ndarray) -> tuple[float, float]:
        """Compute mean chi-square and genomic control lambda.

        Args:
            chisq (np.ndarray): Chi-square statistics of shape (n_snp, 1).

        Returns:
            tuple[float, float]: Mean chi-square and lambda_GC.
        """
        mean_chisq = float(np.mean(chisq))
        lambda_gc = float(np.median(np.asarray(chisq)) / 0.4549)
        return mean_chisq, lambda_gc

    def _ratio(
        self, intercept: float, intercept_se: float, mean_chisq: float
    ) -> tuple[float | None, float | None]:
        """Compute LDSC ratio (intercept - 1) / (mean chi-square - 1).

        Args:
            intercept (float): LDSC intercept estimate.
            intercept_se (float): Standard error of the intercept.
            mean_chisq (float): Mean chi-square across SNPs.

        Returns:
            tuple[float | None, float | None]:
                ratio, ratio_se, or (None, None) if mean chi-square <= 1.
        """
        if mean_chisq > 1:
            ratio_se = intercept_se / (mean_chisq - 1)
            ratio = (intercept - 1) / (mean_chisq - 1)
        else:
            ratio = None
            ratio_se = None
        return ratio, ratio_se

    def _update_weights(
        self,
        ld: np.ndarray,
        w_ld: np.ndarray,
        N: np.ndarray,
        M: float,
        hsq: float,
        intercept: float | None,
        ii: np.ndarray | None = None,
    ) -> np.ndarray:
        """Helper used by the base class to compute initial weights.

        Args:
            ld (np.ndarray): LD scores of shape (n_snp, 1).
            w_ld (np.ndarray): LD-based weighting LD scores of shape
                (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (float): Total number of SNPs used for LD scores.
            hsq (float): Initial heritability estimate.
            intercept (float | None): Intercept used in the weight formula.
            ii (np.ndarray | None): Unused. Present for API compatibility.

        Returns:
            np.ndarray: Initial weights of shape (n_snp, 1).
        """
        if intercept is None:
            intercept = self.__null_intercept__
        return self.weights(ld, w_ld, N, M, hsq, intercept, ii)

    @classmethod
    def weights(
        cls,
        ld: np.ndarray,
        w_ld: np.ndarray,
        N: np.ndarray,
        M: float,
        hsq: float,
        intercept: float | None = None,
        ii: np.ndarray | None = None,
    ) -> np.ndarray:
        """Compute LDSC regression weights for heritability estimation.

        The weights approximate the inverse of the conditional variance of the
        chi-square statistics given LD scores.

        Args:
            ld (np.ndarray): LD scores for each SNP of shape (n_snp, 1).
            w_ld (np.ndarray): LD-based weighting LD scores of shape
                (n_snp, 1).
            N (np.ndarray): Sample sizes of shape (n_snp, 1).
            M (float): Total number of SNPs used to compute LD scores.
            hsq (float): Current heritability estimate, clipped to [0, 1].
            intercept (float | None): Intercept used in the heteroskedastic
                variance model. Defaults to 1 if None.
            ii (np.ndarray | None): Unused. Present for API compatibility.

        Returns:
            np.ndarray: Regression weights of shape (n_snp, 1).
        """
        M = float(M)
        if intercept is None:
            intercept = 1.0

        hsq = max(hsq, 0.0)
        hsq = min(hsq, 1.0)
        ld = np.fmax(ld, 1.0)
        w_ld = np.fmax(w_ld, 1.0)
        c = hsq * N / M
        het_w = 1.0 / (2 * np.square(intercept + np.multiply(c, ld)))
        oc_w = 1.0 / w_ld
        w = np.multiply(het_w, oc_w)
        return w

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
