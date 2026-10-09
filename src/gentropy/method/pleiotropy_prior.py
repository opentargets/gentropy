"""Gene-level prior of pleiotropy, predicted from gene features with a ridge regression.

The model follows the polygenic priority score (PoPS; Weeks et al. 2023, Nat Genet
55:1267-1276): a ridge regression of a gene-level target on gene features, fitted without the
chromosome of the gene being scored. PoPS regresses MAGMA gene z-scores, which need full
summary statistics. Here the target is the number of diseases a gene is the nearest gene for,
computed from credible sets alone, so the prior can be built for every release.

`PleiotropyPrior.approx_pops` is the per-disease variant: one ridge regression per disease of
its 0/1 nearest-gene labels on the top principal components of the gene features, scored by
leave-one-out.

This module is written from the formulas in the documentation of
[`PleiotropyPrior`][gentropy.method.pleiotropy_prior.PleiotropyPrior]; it does not reuse the PoPS
code, which is released under GPL-3.0.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import numpy as np
import scipy.linalg
import scipy.sparse
import scipy.stats

logger = logging.getLogger(__name__)


@dataclass
class ApproxPopsFit:
    """Result of the per-disease ridge regressions on the top principal components.

    Attributes:
        scores (np.ndarray): `genes x diseases` leave-one-out scores, z-scored within each
            disease, for the diseases that pass the AUC filter
        kept (np.ndarray): Which of the input diseases pass the filter
        auc (np.ndarray): Leave-one-out AUC of every input disease
        auc_lower_bound (np.ndarray): Lower bound of its 95% confidence interval
        lambdas (np.ndarray): Ridge penalty chosen for every input disease
    """

    scores: np.ndarray
    kept: np.ndarray
    auc: np.ndarray
    auc_lower_bound: np.ndarray
    lambdas: np.ndarray


@dataclass
class PleiotropyPriorFit:
    """Result of the leave-one-chromosome-out kernel ridge regression.

    Attributes:
        scores (np.ndarray): Out-of-chromosome prediction for every gene, in the order of the
            input
        lambdas (dict[str, float]): Ridge penalty chosen by generalised cross-validation for
            each held-out chromosome
    """

    scores: np.ndarray
    lambdas: dict[str, float]


class PleiotropyPrior:
    """Leave-one-chromosome-out kernel ridge regression of a gene-level target on gene features.

    Notation: `X` is the `n x p` matrix of gene features with every column standardised across
    the `n` genes, `C` the matrix of covariates plus an intercept, `y` the target and `chr(g)` the
    chromosome of gene `g`. For each chromosome `k`, the training genes `T` are the genes on any
    other chromosome that are allowed in the fit and the held-out genes `H` are the genes on `k`:

        r_T  = (I - C_T (C_T' C_T)^-1 C_T') y_T          covariates projected out within the fold
        K    = X X'                                     linear kernel over all genes
        K_TT = U diag(s) U'                             eigendecomposition
        a_k  = U diag(1 / (s + lambda_k)) U' r_T
        score_g = K_gT a_k                              for every g in H

    This is the dual form of a ridge regression of `r_T` on `X_T` without intercept, so
    `score_g = x_g' beta^(-k)` where `beta^(-k)` is fitted without chromosome `k`. A gene's own
    target never reaches its own score: neither the covariate fit nor the ridge fit sees it.

    `lambda_k` minimises the generalised cross-validation criterion, computed from the same
    eigendecomposition with `u = U' r_T` and `n_T = |T|`:

        GCV_k(lambda) = n_T * sum_i (lambda / (s_i + lambda))^2 u_i^2
                        / (n_T - sum_i s_i / (s_i + lambda))^2

    The kernel form never holds the dense `n x p` feature matrix in memory: `K` is accumulated
    over column chunks, so the cost is set by the number of genes (about 20,000), not by the
    number of features.
    """

    DEFAULT_LAMBDA_GRID: tuple[float, ...] = tuple(
        float(v) for v in np.logspace(-2, 10, 25)
    )
    """Ridge penalties searched by default, `10^-2` to `10^10` in steps of `10^0.5`."""

    @staticmethod
    def column_chunks(
        matrix: scipy.sparse.spmatrix | scipy.sparse.sparray, chunk_size: int = 500
    ) -> Iterator[np.ndarray]:
        """Split a sparse `genes x features` matrix into dense column chunks.

        Args:
            matrix (scipy.sparse.spmatrix | scipy.sparse.sparray): Feature matrix
            chunk_size (int): Number of columns per chunk

        Yields:
            np.ndarray: Dense `genes x <=chunk_size` chunk
        """
        columns = scipy.sparse.csc_array(matrix)
        for start in range(0, columns.shape[1], chunk_size):
            yield columns[:, start : start + chunk_size].toarray()

    @staticmethod
    def standardise(matrix: np.ndarray) -> np.ndarray:
        """Centre and scale every column to mean 0 and variance 1, dropping constant columns.

        Args:
            matrix (np.ndarray): `genes x features` matrix

        Returns:
            np.ndarray: Standardised matrix without the zero-variance columns
        """
        sd = matrix.std(axis=0)
        keep = sd > 0
        return (matrix[:, keep] - matrix[:, keep].mean(axis=0)) / sd[keep]

    @classmethod
    def accumulate_kernel(
        cls: type[PleiotropyPrior],
        chunks: Iterable[np.ndarray],
        column_weights: np.ndarray | None = None,
    ) -> tuple[np.ndarray, int]:
        """Accumulate the linear kernel `K = X X'` over column chunks of `X`.

        Each chunk is standardised on its own, which is the same as standardising `X` as a
        whole because standardisation acts column by column.

        Args:
            chunks (Iterable[np.ndarray]): `genes x columns` chunks of the feature matrix, all
                with the same rows
            column_weights (np.ndarray | None): Weight of every column of `X`, applied after
                standardisation. Defaults to 1 for every column.

        Returns:
            tuple[np.ndarray, int]: The `genes x genes` kernel and the number of features
                that entered it

        Raises:
            ValueError: If no chunk holds a non-constant column
        """
        kernel: np.ndarray | None = None
        n_features = 0
        start = 0
        for chunk in chunks:
            width = chunk.shape[1]
            standardised = cls.standardise(chunk)
            if column_weights is not None:
                weights = column_weights[start : start + width][chunk.std(axis=0) > 0]
                standardised = standardised * weights
            start += width
            if standardised.shape[1] == 0:
                continue
            n_features += standardised.shape[1]
            if kernel is None:
                kernel = standardised @ standardised.T
            else:
                kernel += standardised @ standardised.T
        if kernel is None:
            raise ValueError("The feature matrix has no column with any variance.")
        return kernel, n_features

    @staticmethod
    def residualise(y: np.ndarray, covariates: np.ndarray | None) -> np.ndarray:
        """Project an intercept and the covariates out of a target.

        Args:
            y (np.ndarray): Target, one value per gene
            covariates (np.ndarray | None): `genes x covariates` matrix, without intercept

        Returns:
            np.ndarray: Residual of the least-squares fit of `y` on the intercept and covariates
        """
        design = np.ones((y.shape[0], 1))
        if covariates is not None and covariates.size:
            design = np.column_stack([design, covariates])
        beta, *_ = np.linalg.lstsq(design, y, rcond=None)
        return y - design @ beta

    @staticmethod
    def generalised_cross_validation(
        eigenvalues: np.ndarray, projected_target: np.ndarray, lambdas: np.ndarray
    ) -> np.ndarray:
        """Generalised cross-validation criterion of a kernel ridge fit for a grid of penalties.

        Args:
            eigenvalues (np.ndarray): Eigenvalues `s` of the training kernel
            projected_target (np.ndarray): Target in the eigenbasis, `u = U' r`
            lambdas (np.ndarray): Penalties to evaluate

        Returns:
            np.ndarray: `GCV(lambda)` for every penalty in the grid
        """
        n = eigenvalues.shape[0]
        # Clip the tiny negative eigenvalues that rounding leaves on a rank-deficient kernel.
        s = np.clip(eigenvalues, 0.0, None)[None, :]
        shrink = lambdas[:, None] / (s + lambdas[:, None])
        rss = (shrink**2 * projected_target[None, :] ** 2).sum(axis=1)
        degrees_of_freedom = (s / (s + lambdas[:, None])).sum(axis=1)
        return n * rss / (n - degrees_of_freedom) ** 2

    @classmethod
    def loco_kernel_ridge(
        cls: type[PleiotropyPrior],
        kernel: np.ndarray,
        y: np.ndarray,
        chromosomes: np.ndarray,
        *,
        covariates: np.ndarray | None = None,
        fit_mask: np.ndarray | None = None,
        lambdas: Iterable[float] = DEFAULT_LAMBDA_GRID,
    ) -> PleiotropyPriorFit:
        """Score every gene with a ridge regression fitted without its chromosome.

        Args:
            kernel (np.ndarray): `genes x genes` linear kernel of the standardised features
            y (np.ndarray): Target, one value per gene
            chromosomes (np.ndarray): Chromosome of every gene
            covariates (np.ndarray | None): `genes x covariates` matrix projected out of the
                target within each fold, without intercept
            fit_mask (np.ndarray | None): Genes allowed in the training folds. Genes outside the
                mask are still scored. Defaults to every gene.
            lambdas (Iterable[float]): Ridge penalties searched by generalised cross-validation

        Returns:
            PleiotropyPriorFit: Scores and the penalty chosen for every chromosome

        Raises:
            ValueError: If the inputs disagree on the number of genes
        """
        n = y.shape[0]
        if kernel.shape != (n, n) or chromosomes.shape[0] != n:
            raise ValueError("kernel, y and chromosomes must describe the same genes.")
        if covariates is not None and covariates.shape[0] != n:
            raise ValueError("covariates must have one row per gene.")
        fit_mask = np.ones(n, dtype=bool) if fit_mask is None else fit_mask
        grid = np.asarray(sorted(lambdas), dtype=np.float64)

        scores = np.zeros(n)
        chosen: dict[str, float] = {}
        for chromosome in sorted(set(chromosomes.tolist())):
            held_out = chromosomes == chromosome
            train = ~held_out & fit_mask
            if not train.any():
                continue
            r_train = cls.residualise(
                y[train], None if covariates is None else covariates[train]
            )
            # Divide-and-conquer in single precision is about 2.5x faster than in double and
            # about 12x faster than the MRRR driver. The rest of the fit stays in double.
            eigenvalues, eigenvectors = scipy.linalg.eigh(
                kernel[np.ix_(train, train)].astype(np.float32),
                overwrite_a=True,
                check_finite=False,
                driver="evd",
            )
            eigenvalues = eigenvalues.astype(np.float64)
            eigenvectors = eigenvectors.astype(np.float64)
            projected = eigenvectors.T @ r_train
            gcv = cls.generalised_cross_validation(eigenvalues, projected, grid)
            best = int(np.argmin(gcv))
            if best in (0, grid.size - 1):
                logger.warning(
                    "Chromosome %s: the ridge penalty chosen, %.3g, is at the edge of the "
                    "grid; consider widening it.",
                    chromosome,
                    grid[best],
                )
            weights = eigenvectors @ (
                projected / (np.clip(eigenvalues, 0.0, None) + grid[best])
            )
            del eigenvectors
            scores[held_out] = kernel[np.ix_(held_out, train)] @ weights
            chosen[str(chromosome)] = float(grid[best])
        return PleiotropyPriorFit(scores=scores, lambdas=chosen)

    @staticmethod
    def top_components(
        kernel: np.ndarray, n_components: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Leading eigenvalues and eigenvectors of a kernel, largest first.

        The eigendecomposition runs in single precision with the divide-and-conquer driver,
        as in [`loco_kernel_ridge`][gentropy.method.pleiotropy_prior.PleiotropyPrior.loco_kernel_ridge].

        Args:
            kernel (np.ndarray): `genes x genes` linear kernel
            n_components (int): Number of components to keep

        Returns:
            tuple[np.ndarray, np.ndarray]: Eigenvalues `s` and `genes x n_components`
                eigenvectors `U`, in double precision
        """
        eigenvalues, eigenvectors = scipy.linalg.eigh(
            kernel.astype(np.float32), overwrite_a=True, check_finite=False, driver="evd"
        )
        order = np.argsort(eigenvalues)[::-1][:n_components]
        return (
            np.clip(eigenvalues[order].astype(np.float64), 0.0, None),
            eigenvectors[:, order].astype(np.float64),
        )

    @staticmethod
    def auc_with_lower_bound(
        scores: np.ndarray, labels: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """AUC of every column of `scores` against the 0/1 labels of the same column.

        The lower bound is `AUC - 1.96 SE`, with the standard error of Hanley and McNeil
        (1982, Radiology 143:29-36).

        Args:
            scores (np.ndarray): `genes x diseases` scores
            labels (np.ndarray): `genes x diseases` 0/1 labels

        Returns:
            tuple[np.ndarray, np.ndarray]: AUC and its 95% lower bound for every column
        """
        ranks = scipy.stats.rankdata(scores, axis=0)
        n_pos = labels.sum(axis=0)
        n_neg = labels.shape[0] - n_pos
        auc = ((ranks * labels).sum(axis=0) - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
        q1 = auc / (2 - auc)
        q2 = 2 * auc**2 / (1 + auc)
        variance = (
            auc * (1 - auc) + (n_pos - 1) * (q1 - auc**2) + (n_neg - 1) * (q2 - auc**2)
        ) / (n_pos * n_neg)
        return auc, auc - 1.96 * np.sqrt(variance)

    @classmethod
    def approx_pops(
        cls: type[PleiotropyPrior],
        eigenvalues: np.ndarray,
        eigenvectors: np.ndarray,
        labels: np.ndarray,
        covariates: np.ndarray | None = None,
        lambdas: Iterable[float] = DEFAULT_LAMBDA_GRID,
    ) -> ApproxPopsFit:
        """Ridge regression of every disease's nearest-gene labels on the top components.

        Notation: `U` are the top `k` eigenvectors of the kernel and `s` their eigenvalues, so
        `U diag(sqrt(s))` are the gene scores on the top `k` principal components of the
        features. For every disease, with `r` its 0/1 labels with an intercept and the
        covariates projected out and `u = U' r`:

            w_i     = s_i / (s_i + lambda)
            fitted  = U diag(w) u
            h_g     = sum_i w_i U_gi^2                  influence of gene g on its own score
            loo_g   = (fitted_g - h_g r_g) / (1 - h_g)

        `loo_g` is exactly the score gene `g` gets from the same ridge fitted without `g`, so a
        gene's own label never reaches its own score. `lambda` minimises generalised
        cross-validation, `n RSS / (n - sum_i w_i)^2` with `RSS = |r|^2 - |u|^2 +
        sum_i (1 - w_i)^2 u_i^2`.

        A disease is kept when the 95% lower bound of the AUC of its leave-one-out scores is
        above 0.5, i.e. when the gene features predict its nearest genes better than chance.
        The scores of a kept disease are then z-scored across genes, so that diseases with
        different numbers of nearest genes are on the same scale.

        Args:
            eigenvalues (np.ndarray): Eigenvalues `s` of the top components
            eigenvectors (np.ndarray): `genes x k` eigenvectors `U`
            labels (np.ndarray): `genes x diseases` 0/1 matrix, 1 when the gene is a nearest
                gene of the disease
            covariates (np.ndarray | None): `genes x covariates` matrix projected out of the
                labels, without intercept
            lambdas (Iterable[float]): Ridge penalties searched by generalised cross-validation

        Returns:
            ApproxPopsFit: Scores of the kept diseases and the diagnostics of every disease
        """
        grid = np.asarray(sorted(lambdas), dtype=np.float64)
        n = labels.shape[0]
        residuals = cls.residualise(labels, covariates)
        projected = eigenvectors.T @ residuals
        outside = (residuals**2).sum(axis=0) - (projected**2).sum(axis=0)
        shrink = eigenvalues[None, :] / (eigenvalues[None, :] + grid[:, None])
        rss = outside[None, :] + ((1 - shrink) ** 2) @ projected**2
        gcv = n * rss / (n - shrink.sum(axis=1))[:, None] ** 2
        best = gcv.argmin(axis=0)
        weights = shrink[best].T
        fitted = eigenvectors @ (weights * projected)
        leverage = eigenvectors**2 @ weights
        loo = (fitted - leverage * residuals) / (1 - leverage)
        auc, lower = cls.auc_with_lower_bound(loo, labels)
        kept = lower > 0.5
        scores = loo[:, kept]
        scores = (scores - scores.mean(axis=0)) / scores.std(axis=0)
        return ApproxPopsFit(
            scores=scores,
            kept=kept,
            auc=auc,
            auc_lower_bound=lower,
            lambdas=grid[best],
        )
