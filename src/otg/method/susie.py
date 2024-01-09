"""Step to run study locus fine-mapping with SuSiE-inf."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import scipy.linalg
import scipy.special
from scipy.optimize import minimize, minimize_scalar


@dataclass
class SUSIE:
    """SuSiE fine-mapping of a study locus from fine-mapping-inf package.

    Note: code copied from fine-mapping-inf package as a placeholder
    https://github.com/FinucaneLab/fine-mapping-inf
    """

    @staticmethod
    def susie(  # noqa: C901
        z: np.ndarray,
        meansq: np.ndarray,
        n: int = 100000,
        L: int = 10,
        LD: np.ndarray | None = None,
        V: np.ndarray | None = None,
        Dsq: np.ndarray | None = None,
        est_ssq: bool = True,
        ssq: np.ndarray | None = None,
        ssq_range: tuple[float, float] | None = None,
        pi0: np.ndarray | None = None,
        est_sigmasq: bool = True,
        est_tausq: bool = True,
        sigmasq: float = 1,
        tausq: float = 0,
        sigmasq_range: tuple[float, float] | None = None,
        tausq_range: tuple[float, float] | None = None,
        PIP: np.ndarray | None = None,
        mu: np.ndarray | None = None,
        method: str = "moments",
        maxiter: int = 100,
        PIP_tol: float = 0.001,
    ) -> dict[str, Any]:
        """Susie with random effects.

        Args:
            z (np.ndarray): vector of z-scores (equal to X'y/sqrt(n))
            meansq (np.ndarray): average squared magnitude of y (equal to ||y||^2/n)
            n (int): sample size
            L (int): number of modeled causal effects
            LD (np.ndarray | None): LD matrix (equal to X'X/n)
            V (np.ndarray | None): precomputed p x p matrix of eigenvectors of X'X
            Dsq (np.ndarray | None): precomputed length-p vector of eigenvalues of X'X
            est_ssq (bool): estimate prior effect size variances s^2 using MLE
            ssq (np.ndarray | None): length-L initialization s^2 for each effect
            ssq_range (tuple[float, float] | None): lower and upper bounds for each s^2, if estimated
            pi0 (np.ndarray | None): length-p vector of prior causal probability for each SNP; must sum to 1
            est_sigmasq (bool): estimate variance sigma^2
            est_tausq (bool): estimate both variances sigma^2 and tau^2
            sigmasq (float): initial value for sigma^2
            tausq (float): initial value for tau^2
            sigmasq_range (tuple[float, float] | None): lower and upper bounds for sigma^2, if estimated using MLE
            tausq_range (tuple[float, float] | None): lower and upper bounds for tau^2, if estimated using MLE
            PIP (np.ndarray | None): p x L initializations of PIPs
            mu (np.ndarray | None): p x L initializations of mu
            method (str): one of {'moments','MLE'}
            maxiter (int): maximum number of SuSiE iterations
            PIP_tol (float): convergence threshold for PIP difference between iterations

        Returns:
            dict[str, Any]: Dictionary with keys:
                PIP -- p x L matrix of PIPs, individually for each effect
                mu -- p x L matrix of posterior means conditional on causal
                omega -- p x L matrix of posterior precisions conditional on causal
                lbf_variable -- p x L matrix of log-Bayes-factors, for each effect
                ssq -- length-L array of final effect size variances s^2
                sigmasq -- final value of sigma^2
                tausq -- final value of tau^2
                alpha -- length-p array of posterior means of infinitesimal effects

        Raises:
            RuntimeError: if missing LD
            RuntimeError: if unsupported variance estimation method
        """
        p = len(z)
        # Precompute V,D^2 in the SVD X=UDV', and V'X'y and y'y
        if (V is None or Dsq is None) and LD is None:
            raise RuntimeError("Missing LD")
        elif V is None or Dsq is None:
            eigvals, V = scipy.linalg.eigh(LD)
            Dsq = np.maximum(n * eigvals, 0)
        else:
            Dsq = np.maximum(Dsq, 0)
        Xty = np.sqrt(n) * z
        VtXty = V.T.dot(Xty)
        yty = n * meansq
        # Initialize diagonal variances, diag(X' Omega X), X' Omega y
        var = tausq * Dsq + sigmasq
        diagXtOmegaX = np.sum(V**2 * (Dsq / var), axis=1)
        XtOmegay = V.dot(VtXty / var)
        # Initialize s_l^2, PIP_j, mu_j, omega_j
        if ssq is None:
            ssq = np.ones(L) * 0.2
        if PIP is None:
            PIP = np.ones((p, L)) / p
        if mu is None:
            mu = np.zeros((p, L))
        lbf_variable = np.zeros((p, L))
        omega = diagXtOmegaX[:, np.newaxis] + 1 / ssq
        # Initialize prior causal probabilities
        if pi0 is None:
            logpi0 = np.ones(p) * np.log(1.0 / p)
        else:
            logpi0 = -np.ones(p) * np.inf
            inds = np.nonzero(pi0 > 0)[0]
            logpi0[inds] = np.log(pi0[inds])

        ####### Main SuSiE iteration loop ######
        def f(x: float) -> float:
            """Negative ELBO as function of x = sigma_e^2.

            Args:
                x (float): sigma_e^2

            Returns:
                float: negative ELBO as function of x = sigma_e^2
            """
            return -scipy.special.logsumexp(
                -0.5 * np.log(1 + x * diagXtOmegaX)
                + x * XtOmegar**2 / (2 * (1 + x * diagXtOmegaX))
                + logpi0
            )

        for it in range(maxiter):
            PIP_prev = PIP.copy()
            # Single effect regression for each effect l = 1,...,L
            for _l in range(L):
                # Compute X' Omega r_l for residual r_l
                b = np.sum(mu * PIP, axis=1) - mu[:, _l] * PIP[:, _l]
                XtOmegaXb = V.dot(V.T.dot(b) * Dsq / var)
                XtOmegar = XtOmegay - XtOmegaXb
                if est_ssq:
                    # Update prior variance ssq[l]
                    res = minimize_scalar(f, bounds=ssq_range, method="bounded")
                    if res.success:
                        ssq[_l] = res.x
                # Update omega, mu, and PIP
                omega[:, _l] = diagXtOmegaX + 1 / ssq[_l]
                mu[:, _l] = XtOmegar / omega[:, _l]
                lbf_variable[:, _l] = XtOmegar**2 / (2 * omega[:, _l]) - 0.5 * np.log(
                    omega[:, _l] * ssq[_l]
                )
                logPIP = lbf_variable[:, _l] + logpi0
                PIP[:, _l] = np.exp(logPIP - scipy.special.logsumexp(logPIP))
            # Update variance components
            if est_sigmasq or est_tausq:
                if method == "moments":
                    (sigmasq, tausq) = SUSIE._MoM(
                        PIP,
                        mu,
                        omega,
                        sigmasq,
                        tausq,
                        n,
                        V,
                        Dsq,
                        VtXty,
                        Xty,
                        yty,
                        est_sigmasq,
                        est_tausq,
                    )
                elif method == "MLE":
                    (sigmasq, tausq) = SUSIE._MLE(
                        PIP,
                        mu,
                        omega,
                        sigmasq,
                        tausq,
                        n,
                        V,
                        Dsq,
                        VtXty,
                        yty,
                        est_sigmasq,
                        est_tausq,
                        it,
                        sigmasq_range,
                        tausq_range,
                    )
                else:
                    raise RuntimeError("Unsupported variance estimation method")
                # Update X' Omega X, X' Omega y
                var = tausq * Dsq + sigmasq
                diagXtOmegaX = np.sum(V**2 * (Dsq / var), axis=1)
                XtOmegay = V.dot(VtXty / var)
            # Determine convergence from PIP differences
            PIP_diff = np.max(np.abs(PIP_prev - PIP))
            if PIP_diff < PIP_tol:
                break
        # Compute posterior means of b and alpha
        b = np.sum(mu * PIP, axis=1)
        XtOmegaXb = V.dot(V.T.dot(b) * Dsq / var)
        XtOmegar = XtOmegay - XtOmegaXb
        alpha = tausq * XtOmegar
        return {
            "PIP": PIP,
            "mu": mu,
            "omega": omega,
            "lbf_variable": lbf_variable,
            "ssq": ssq,
            "sigmasq": sigmasq,
            "tausq": tausq,
            "alpha": alpha,
        }

    @staticmethod
    def _MoM(
        PIP: np.ndarray,
        mu: np.ndarray,
        omega: np.ndarray,
        sigmasq: float,
        tausq: float,
        n: int,
        V: np.ndarray,
        Dsq: np.ndarray,
        VtXty: np.ndarray,
        Xty: np.ndarray,
        yty: float,
        est_sigmasq: bool,
        est_tausq: bool,
    ) -> tuple[float, float]:
        """Subroutine to estimate sigma^2, tau^2 using method-of-moments.

        Args:
            PIP (np.ndarray): p x L matrix of PIPs
            mu (np.ndarray): p x L matrix of posterior means conditional on causal
            omega (np.ndarray): p x L matrix of posterior precisions conditional on causal
            sigmasq (float): initial value for sigma^2
            tausq (float): initial value for tau^2
            n (int): sample size
            V (np.ndarray): precomputed p x p matrix of eigenvectors of X'X
            Dsq (np.ndarray): precomputed length-p vector of eigenvalues of X'X
            VtXty (np.ndarray): precomputed length-p vector V'X'y
            Xty (np.ndarray): precomputed length-p vector X'y
            yty (float): precomputed y'y
            est_sigmasq (bool): estimate variance sigma^2
            est_tausq (bool): estimate both variances sigma^2 and tau^2

        Returns:
            tuple[float, float]: (sigmasq,tausq) tuple of updated variances
        """
        (p, L) = mu.shape
        # Compute A
        A = np.array([[n, sum(Dsq)], [0, sum(Dsq**2)]])
        A[1, 0] = A[0, 1]
        # Compute diag(V'MV)
        b = np.sum(mu * PIP, axis=1)
        Vtb = V.T.dot(b)
        diagVtMV = Vtb**2
        tmpD = np.zeros(p)
        for _l in range(L):
            bl = mu[:, _l] * PIP[:, _l]
            Vtbl = V.T.dot(bl)
            diagVtMV -= Vtbl**2
            tmpD += PIP[:, _l] * (mu[:, _l] ** 2 + 1 / omega[:, _l])
        diagVtMV += np.sum((V.T) ** 2 * tmpD, axis=1)
        # Compute x
        x = np.zeros(2)
        x[0] = yty - 2 * sum(b * Xty) + sum(Dsq * diagVtMV)
        x[1] = sum(Xty**2) - 2 * sum(Vtb * VtXty * Dsq) + sum(Dsq**2 * diagVtMV)
        if est_tausq:
            sol = scipy.linalg.solve(A, x)
            if sol[0] > 0 and sol[1] > 0:
                (sigmasq, tausq) = sol
            else:
                (sigmasq, tausq) = (x[0] / n, 0)
        elif est_sigmasq:
            sigmasq = (x[0] - A[0, 1] * tausq) / n
        return sigmasq, tausq

    @staticmethod
    def _MLE(
        PIP: np.ndarray,
        mu: np.ndarray,
        omega: np.ndarray,
        sigmasq: float,
        tausq: float,
        n: int,
        V: np.ndarray,
        Dsq: np.ndarray,
        VtXty: np.ndarray,
        yty: float,
        est_sigmasq: bool,
        est_tausq: bool,
        it: int,
        sigmasq_range: tuple[float, float] | None = None,
        tausq_range: tuple[float, float] | None = None,
    ) -> tuple[float, float]:
        """Subroutine to estimate sigma^2, tau^2 using MLE.

        Args:
            PIP (np.ndarray): p x L matrix of PIPs
            mu (np.ndarray): p x L matrix of posterior means conditional on causal
            omega (np.ndarray): p x L matrix of posterior precisions conditional on causal
            sigmasq (float): initial value for sigma^2
            tausq (float): initial value for tau^2
            n (int): sample size
            V (np.ndarray): precomputed p x p matrix of eigenvectors of X'X
            Dsq (np.ndarray): precomputed length-p vector of eigenvalues of X'X
            VtXty (np.ndarray): precomputed length-p vector V'X'y
            yty (float): precomputed y'y
            est_sigmasq (bool): estimate variance sigma^2
            est_tausq (bool): estimate both variances sigma^2 and tau^2
            it (int): iteration number
            sigmasq_range (tuple[float, float] | None): lower and upper bounds for sigma^2, if estimated using MLE
            tausq_range (tuple[float, float] | None): lower and upper bounds for tau^2, if estimated using MLE

        Returns:
            tuple[float, float]: (sigmasq,tausq) tuple of updated variances
        """
        (p, L) = mu.shape
        if sigmasq_range is None:
            sigmasq_range = (0.2 * yty / n, 1.2 * yty / n)
        if tausq_range is None:
            tausq_range = (1e-12, 1.2 * yty / (n * p))
        # Compute diag(V'MV)
        b = np.sum(mu * PIP, axis=1)
        Vtb = V.T.dot(b)
        diagVtMV = Vtb**2
        tmpD = np.zeros(p)
        for _l in range(L):
            bl = mu[:, _l] * PIP[:, _l]
            Vtbl = V.T.dot(bl)
            diagVtMV -= Vtbl**2
            tmpD += PIP[:, _l] * (mu[:, _l] ** 2 + 1 / omega[:, _l])
        diagVtMV += np.sum((V.T) ** 2 * tmpD, axis=1)

        # negative ELBO as function of x = (sigma_e^2,sigma_g^2)
        def f(x: tuple[float, float]) -> float:
            """Negative ELBO as function of x = (sigma_e^2,sigma_g^2).

            Args:
                x (tuple[float, float]): (sigma_e^2,sigma_g^2)

            Returns:
                float: negative ELBO as function of x = (sigma_e^2,sigma_g^2)
            """
            return (
                0.5 * (n - p) * np.log(x[0])
                + 0.5 / x[0] * yty
                + np.sum(
                    0.5 * np.log(x[1] * Dsq + x[0])
                    - 0.5 * x[1] / x[0] * VtXty**2 / (x[1] * Dsq + x[0])
                    - Vtb * VtXty / (x[1] * Dsq + x[0])
                    + 0.5 * Dsq / (x[1] * Dsq + x[0]) * diagVtMV
                )
            )

        if est_tausq:
            res = minimize(
                f,
                (sigmasq, tausq),
                method="L-BFGS-B",
                bounds=(sigmasq_range, tausq_range),
            )
            if res.success:
                sigmasq, tausq = res.x
        elif est_sigmasq:

            def g(x: float) -> float:
                """Negative ELBO as function of x = sigma_e^2.

                Args:
                    x (float): sigma_e^2

                Returns:
                    float: negative ELBO as function of x = sigma_e^2
                """
                return f((x, tausq))

            res = minimize(g, sigmasq, method="L-BFGS-B", bounds=(sigmasq_range,))
            if res.success:
                sigmasq = res.x
        return sigmasq, tausq
