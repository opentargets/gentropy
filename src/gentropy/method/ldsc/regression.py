"""LDSC regression classes: LD_Score_Regression, Hsq, and GeneticCov."""
from __future__ import annotations

from collections import namedtuple
from typing import Any

import numpy as np

from gentropy.method.ldsc.irwls import IRWLS
from gentropy.method.ldsc.jackknife import Jackknife, LstsqJackknifeFast
from gentropy.method.ldsc.utils import append_intercept, update_separators


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
        s: list[int] = update_separators(step1_jknife.separators, step1_ii).tolist()
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
        w = self.weights(ld, w_ld, N, M, float(np.asarray(hsq).item()), intercept_eff, ii)

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


class GeneticCov(LD_Score_Regression):
    """LDSC-style genetic covariance regression for two traits.

    Regresses the product of z-scores (z1 * z2) on LD scores to estimate
    the genetic covariance (gcov) between two traits.
    """

    __null_intercept__ = 0.0

    def __init__(
        self,
        y: np.ndarray,
        x: np.ndarray,
        w: np.ndarray,
        N1: np.ndarray,
        N2: np.ndarray,
        M: np.ndarray,
        n_blocks: int = 200,
        intercept: float | None = None,
        hsq1: float = 0.0,
        hsq2: float = 0.0,
        slow: bool = False,
        twostep: float | None = None,
        old_weights: bool = False,
    ) -> None:
        """Initialise genetic covariance regression.

        Args:
            y (np.ndarray): Product of z-scores z1*z2, shape (n_snp, 1).
            x (np.ndarray): LD-score design matrix, shape (n_snp, n_annot).
            w (np.ndarray): LD-based weights, shape (n_snp, 1).
            N1 (np.ndarray): Sample sizes for trait 1, shape (n_snp, 1).
            N2 (np.ndarray): Sample sizes for trait 2, shape (n_snp, 1).
            M (np.ndarray): Row vector of annotation sizes, shape (1, n_annot).
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Fixed intercept. If None, estimated.
            hsq1 (float): Pre-estimated heritability of trait 1 (used for weights).
            hsq2 (float): Pre-estimated heritability of trait 2 (used for weights).
            slow (bool): Kept for compatibility.
            twostep (float | None): If not None, use two-step LDSC.
            old_weights (bool): Use original LDSC weighting scheme if True.
        """
        self._hsq1 = float(hsq1)
        self._hsq2 = float(hsq2)
        self._N1 = N1
        self._N2 = N2

        step1_ii: np.ndarray | None = None
        if twostep is not None:
            step1_ii = np.abs(y) < twostep

        # Pass N1 to base class as the "N" argument (used only for scaling Nbar).
        # The actual weight computation uses self._N1 and self._N2 via _update_func.
        super().__init__(
            y,
            x,
            w,
            N1,
            M,
            n_blocks,
            intercept=intercept,
            slow=slow,
            step1_ii=step1_ii,
            old_weights=old_weights,
        )

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
        """Compute initial weights using both trait heritabilities.

        Args:
            ld (np.ndarray): LD scores, shape (n_snp, 1).
            w_ld (np.ndarray): LD weighting scores, shape (n_snp, 1).
            N (np.ndarray): Sample sizes (N1 passed from base class), shape (n_snp, 1).
            M (float): Total number of SNPs.
            hsq (float): Aggregate gcov estimate (unused; hsq1/hsq2 used instead).
            intercept (float | None): Intercept for weights.
            ii (np.ndarray | None): Unused.

        Returns:
            np.ndarray: Weights of shape (n_snp, 1).
        """
        if intercept is None:
            intercept = self.__null_intercept__
        return self.weights(ld, w_ld, self._N1, self._N2, M, self._hsq1, self._hsq2, intercept)

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
        """Update function for IRWLS in genetic covariance regression.

        Args:
            x (tuple[Any, ...]): Output of np.linalg.lstsq.
            ref_ld_tot (np.ndarray): Total LD design matrix.
            w_ld (np.ndarray): LD weighting scores, shape (n_snp, 1).
            N (np.ndarray): Sample sizes (N1) passed by base class.
            M (float): Total SNP count.
            Nbar (float): Mean sample size.
            intercept (float | None): Fixed intercept, if any.
            ii (np.ndarray | None): Optional SNP mask.

        Returns:
            np.ndarray: Updated weights, shape (n_snp, 1).
        """
        if intercept is None:
            intercept_eff = float(np.asarray(x[0][1]).item()) if len(x[0]) > 1 else 0.0
        else:
            intercept_eff = float(intercept)

        ld = ref_ld_tot[:, 0].reshape(w_ld.shape)

        N1 = self._N1
        N2 = self._N2
        if ii is not None:
            N1 = N1[ii].reshape(-1, 1)
            N2 = N2[ii].reshape(-1, 1)
            # ld and w_ld arrive here already sliced to the step-1 subset
            # (the lambda closure in _run_twostep_ldsc uses x1/w1 which are
            # x[step1_ii] / w[step1_ii]), so do NOT re-index them with ii.

        return self.weights(ld, w_ld, N1, N2, M, self._hsq1, self._hsq2, intercept_eff)

    @classmethod
    def weights(
        cls,
        ld: np.ndarray,
        w_ld: np.ndarray,
        N1: np.ndarray,
        N2: np.ndarray,
        M: float,
        hsq1: float,
        hsq2: float,
        intercept: float = 0.0,
    ) -> np.ndarray:
        """Compute LDSC regression weights for genetic covariance estimation.

        Approximates 1 / Var(z1 * z2) using the LDSC mean model.

        Args:
            ld (np.ndarray): LD scores, shape (n_snp, 1).
            w_ld (np.ndarray): LD weighting scores, shape (n_snp, 1).
            N1 (np.ndarray): Sample sizes for trait 1, shape (n_snp, 1).
            N2 (np.ndarray): Sample sizes for trait 2, shape (n_snp, 1).
            M (float): Total SNP count used for LD scores.
            hsq1 (float): Heritability of trait 1, clipped to [0, 1].
            hsq2 (float): Heritability of trait 2, clipped to [0, 1].
            intercept (float): Cross-trait intercept used in variance approximation.

        Returns:
            np.ndarray: Regression weights, shape (n_snp, 1).
        """
        M = float(M)
        hsq1 = float(np.clip(hsq1, 0.0, 1.0))
        hsq2 = float(np.clip(hsq2, 0.0, 1.0))
        ld = np.fmax(ld, 1.0)
        w_ld = np.fmax(w_ld, 1.0)

        c1 = hsq1 * N1 / M
        c2 = hsq2 * N2 / M
        # Var(z1*z2) ≈ (1 + c1*ld) * (1 + c2*ld) + intercept^2
        het_w = 1.0 / (
            np.multiply(1.0 + np.multiply(c1, ld), 1.0 + np.multiply(c2, ld))
            + intercept ** 2
        )
        oc_w = 1.0 / w_ld
        return np.multiply(het_w, oc_w)
