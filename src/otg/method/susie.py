"""Step to run study locus fine-mapping with SuSiE-inf."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import scipy.linalg
import scipy.special
from scipy.optimize import minimize, minimize_scalar

from otg.common.session import Session


@dataclass
class SuSiE:
    """SuSiE fine-mapping of a study locus from fine-mapping-inf package.

    Note: code copied from fine-mapping-inf package as a placeholder
    https://github.com/FinucaneLab/fine-mapping-inf
    """

    session: Session = Session()

    @staticmethod
    def MoM(
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
        verbose,
    ):
        """Subroutine to estimate sigma^2, tau^2 using method-of-moments"""
        (p, L) = mu.shape
        # Compute A
        A = np.array([[n, sum(Dsq)], [0, sum(Dsq**2)]])
        A[1, 0] = A[0, 1]
        # Compute diag(V'MV)
        b = np.sum(mu * PIP, axis=1)
        Vtb = V.T.dot(b)
        diagVtMV = Vtb**2
        tmpD = np.zeros(p)
        for l in range(L):
            bl = mu[:, l] * PIP[:, l]
            Vtbl = V.T.dot(bl)
            diagVtMV -= Vtbl**2
            tmpD += PIP[:, l] * (mu[:, l] ** 2 + 1 / omega[:, l])
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
            if verbose:
                print("Update (sigma^2,tau^2) to (%f,%e)" % (sigmasq, tausq))
        elif est_sigmasq:
            sigmasq = (x[0] - A[0, 1] * tausq) / n
            if verbose:
                print("Update sigma^2 to %f" % sigmasq)
        return sigmasq, tausq

    def MLE(
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
        sigmasq_range,
        tausq_range,
        it,
        verbose,
    ):
        """Subroutine to estimate sigma^2, tau^2 using MLE"""
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
        for l in range(L):
            bl = mu[:, l] * PIP[:, l]
            Vtbl = V.T.dot(bl)
            diagVtMV -= Vtbl**2
            tmpD += PIP[:, l] * (mu[:, l] ** 2 + 1 / omega[:, l])
        diagVtMV += np.sum((V.T) ** 2 * tmpD, axis=1)

        # negative ELBO as function of x = (sigma_e^2,sigma_g^2)
        def f(x):
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
                if verbose:
                    print("Update (sigma^2,tau^2) to (%f,%e)" % (sigmasq, tausq))
            else:
                print(
                    "WARNING: sigma^2 and tau^2 update for iteration %d failed to converge; keeping previous parameters"
                    % it
                )
        elif est_sigmasq:

            def g(x):
                return f((x, tausq))

            res = minimize(g, sigmasq, method="L-BFGS-B", bounds=(sigmasq_range,))
            if res.success:
                sigmasq = res.x
                if verbose:
                    print("Update sigma^2 to %f" % sigmasq)
            else:
                print(
                    "WARNING: sigma^2 update for iteration %d failed to converge; keeping previous parameters"
                    % it
                )
        return sigmasq, tausq

    def susie(
        z,
        meansq,
        n,
        L,
        LD=None,
        V=None,
        Dsq=None,
        est_ssq=True,
        ssq=None,
        ssq_range=(0, 1),
        pi0=None,
        est_sigmasq=True,
        est_tausq=True,
        sigmasq=1,
        tausq=0,
        method="moments",
        sigmasq_range=None,
        tausq_range=None,
        PIP=None,
        mu=None,
        maxiter=100,
        PIP_tol=1e-3,
        verbose=True,
    ):
        """SuSiE with random effects

        z -- vector of z-scores (equal to X'y/sqrt(n))
        meansq -- average squared magnitude of y (equal to ||y||^2/n)
        n -- sample size
        L -- number of modeled causal effects

        LD -- LD matrix (equal to X'X/n)
        V -- precomputed p x p matrix of eigenvectors of X'X
        Dsq -- precomputed length-p vector of eigenvalues of X'X
        (Must provide either LD or the pair (V,Dsq))

        est_ssq -- estimate prior effect size variances s^2 using MLE
        ssq -- length-L initialization s^2 for each effect
            Default: 0.2 for every effect
        ssq_range -- lower and upper bounds for each s^2, if estimated
        pi0 -- length-p vector of prior causal probability for each SNP; must sum to 1
            Default: 1/p for every SNP

        est_sigmasq -- estimate variance sigma^2
        est_tausq -- estimate both variances sigma^2 and tau^2
        sigmasq -- initial value for sigma^2
        tausq -- initial value for tau^2
        method -- one of {'moments','MLE'}
                (sigma^2,tau^2) are estimated using method-of-moments or MLE
        sigmasq_range -- lower and upper bounds for sigma^2, if estimated using MLE
                        Default: 0.2 * ||y||^2/n to 1.2 * ||y||^2/n
        tausq_range -- lower and upper bounds for tau^2, if estimated using MLE
                    Default: 0 to 1.2 * ||y||^2/(n*p)

        PIP -- p x L initializations of PIPs
            Default: 1/#SNPs for each SNP and effect
        mu -- p x L initializations of mu
            Default: 0 for each SNP and effect

        maxiter -- maximum number of SuSiE iterations
        PIP_tol -- convergence threshold for PIP difference between iterations

        Returns: Dictionary with keys
        PIP -- p x L matrix of PIPs, individually for each effect
        mu -- p x L matrix of posterior means conditional on causal
        omega -- p x L matrix of posterior precisions conditional on causal
        lbf_variable -- p x L matrix of log-Bayes-factors, for each effect
        ssq -- length-L array of final effect size variances s^2
        sigmasq -- final value of sigma^2
        tausq -- final value of tau^2
        alpha -- length-p array of posterior means of infinitesimal effects
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
        for it in range(maxiter):
            if verbose:
                print("Iteration %d" % it)
            PIP_prev = PIP.copy()
            # Single effect regression for each effect l = 1,...,L
            for l in range(L):
                # Compute X' Omega r_l for residual r_l
                b = np.sum(mu * PIP, axis=1) - mu[:, l] * PIP[:, l]
                XtOmegaXb = V.dot(V.T.dot(b) * Dsq / var)
                XtOmegar = XtOmegay - XtOmegaXb
                if est_ssq:
                    # Update prior variance ssq[l]
                    def f(x):
                        return -scipy.special.logsumexp(
                            -0.5 * np.log(1 + x * diagXtOmegaX)
                            + x * XtOmegar**2 / (2 * (1 + x * diagXtOmegaX))
                            + logpi0
                        )

                    res = minimize_scalar(f, bounds=ssq_range, method="bounded")
                    if res.success:
                        ssq[l] = res.x
                        if verbose:
                            print("Update s^2 for effect %d to %f" % (l, ssq[l]))
                    else:
                        print(
                            "WARNING: s^2 update for iteration %d, effect %d failed to converge; keeping previous parameters"
                            % (it, l)
                        )
                # Update omega, mu, and PIP
                omega[:, l] = diagXtOmegaX + 1 / ssq[l]
                mu[:, l] = XtOmegar / omega[:, l]
                lbf_variable[:, l] = XtOmegar**2 / (2 * omega[:, l]) - 0.5 * np.log(
                    omega[:, l] * ssq[l]
                )
                logPIP = lbf_variable[:, l] + logpi0
                PIP[:, l] = np.exp(logPIP - scipy.special.logsumexp(logPIP))
            # Update variance components
            if est_sigmasq or est_tausq:
                if method == "moments":
                    (sigmasq, tausq) = MoM(
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
                        verbose,
                    )
                elif method == "MLE":
                    (sigmasq, tausq) = MLE(
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
                        sigmasq_range,
                        tausq_range,
                        it,
                        verbose,
                    )
                else:
                    raise RuntimeError("Unsupported variance estimation method")
                # Update X' Omega X, X' Omega y
                var = tausq * Dsq + sigmasq
                diagXtOmegaX = np.sum(V**2 * (Dsq / var), axis=1)
                XtOmegay = V.dot(VtXty / var)
            # Determine convergence from PIP differences
            PIP_diff = np.max(np.abs(PIP_prev - PIP))
            if verbose:
                print("Maximum change in PIP: %f" % PIP_diff)
            if PIP_diff < PIP_tol:
                if verbose:
                    print("CONVERGED")
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

    def cred(
        PIP, coverage=0.9, purity=0.5, LD=None, V=None, Dsq=None, n=None, dedup=True
    ):
        """Compute credible sets from single-effect PIPs

        PIP -- p x L PIP matrix output by susie
        coverage -- coverage level for each credible set
        purity -- sets with minimum absolute correlation < purity are removed

        LD -- LD matrix (equal to X'X/n)
        V -- precomputed p x p matrix of eigenvectors of X'X
        Dsq -- precomputed length-p vector of eigenvalues of X'X
        n -- sample size
        dedup -- remove duplicate CS's
        (Must provide either LD or the triple (V,Dsq,n), to do purity filtering)

        Returns: list of variable indices corresponding to credible sets
        """
        if (V is None or Dsq is None or n is None) and LD is None:
            raise RuntimeError("Missing inputs for purity filtering")
        # Compute credible sets
        cred = []
        for l in range(PIP.shape[1]):
            sortinds = np.argsort(PIP[:, l])[::-1]
            ind = min(np.nonzero(np.cumsum(PIP[sortinds, l]) >= coverage)[0])
            credset = sortinds[: (ind + 1)]
            # Filter by purity
            if len(credset) == 1:
                cred.append(list(credset))
                continue
            if len(credset) < 100:
                rows = credset
            else:
                np.random.seed(123)
                rows = np.random.choice(credset, size=100, replace=False)
            if LD is not None:
                LDloc = LD[np.ix_(rows, rows)]
            else:
                LDloc = (V[rows, :] * Dsq).dot(V[rows, :].T) / n
            if np.min(np.abs(LDloc)) > purity:
                cred.append(sorted(list(credset)))
        if dedup:
            cred = list(
                map(
                    list,
                    sorted(set(map(tuple, cred)), key=list(map(tuple, cred)).index),
                )
            )
        return cred

    def run_susie_finemapping(
        z=z,
        filtered_LDMatrix=filtered_LDMatrix,
        n_sample=500000,
    ):
        """Runs the SuSiE fine-mapping."""
        eigenvals, V = scipy.linalg.eigh(filtered_LDMatrix)
        pi0 = None
        susie_output = susie(
            z=z,
            meansq=1,
            n=n_sample,
            L=10,
            LD=filtered_LDMatrix,
            V=None,
            Dsq=None,
            est_ssq=True,
            ssq=None,
            ssq_range=(0, 1),
            pi0=None,
            est_sigmasq=True,
            est_tausq=True,
            sigmasq=1,
            tausq=0,
            method="moments",
            sigmasq_range=None,
            tausq_range=None,
            PIP=None,
            mu=None,
            maxiter=100,
            PIP_tol=1e-3,
            verbose=True,
        )

        susie_output["cred"] = cred(
            susie_output["PIP"],
            coverage=0.9,
            purity=0.1,
            LD=filtered_LDMatrix,
            V=V,
            Dsq=None,
            n=n_sample,
        )
        return susie_output

    def write_tsv(df, out_file):
        """Write tsv file of SuSiE results."""
        df.to_csv(out_file, sep="\t", index=False)

    def process_output(output_dict, df, output_prefix):
        """Organise SuSiE results as output."""
        df["prob"] = 1 - (1 - output_dict["PIP"]).prod(axis=1)
        L = output_dict["PIP"].shape[1]
        alpha_cols = ["alpha{}".format(i) for i in range(1, L + 1)]
        mu_cols = ["mu{}".format(i) for i in range(1, L + 1)]
        df[
            alpha_cols
            + mu_cols
            + ["omega{}".format(i) for i in range(1, L + 1)]
            + ["lbf_variable{}".format(i) for i in range(1, L + 1)]
        ] = pd.DataFrame(
            np.concatenate(
                (
                    output_dict["PIP"],
                    output_dict["mu"],
                    output_dict["omega"],
                    output_dict["lbf_variable"],
                ),
                axis=1,
            )
        )
        df["alpha"] = output_dict["alpha"]
        df["post_mean"] = (
            np.sum(df[mu_cols].values * df[alpha_cols].values, axis=1) + df["alpha"]
        )
        df["tausq"] = output_dict["tausq"]
        df["sigmasq"] = output_dict["sigmasq"]
        df["cs"] = -1
        if len(output_dict["cred"]) > 0:
            df = df.reset_index(drop=True)
            for i, x in enumerate(output_dict["cred"]):
                df.loc[x, "cs"] = i + 1
        out_file = output_prefix + ".susieinf.bgz"
        write_tsv(df, out_file)
