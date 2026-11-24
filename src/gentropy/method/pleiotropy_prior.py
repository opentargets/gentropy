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
import scipy.linalg.blas
import scipy.sparse
import scipy.stats

logger = logging.getLogger(__name__)


@dataclass
class ApproxPopsFit:
    """Result of the per-disease ridge regressions on the top principal components.

    Attributes:
        scores (np.ndarray): `genes x diseases` leave-one-out scores, z-scored within each
            disease, for the diseases that pass the AUC filter
        held_out_scores (np.ndarray): `held-out genes x diseases` scores of the genes kept out
            of the fit, on the same scale as `scores`; no rows when none were given
        kept (np.ndarray): Which of the input diseases pass the filter
        auc (np.ndarray): Leave-one-out AUC of every input disease
        auc_lower_bound (np.ndarray): Lower bound of its 95% confidence interval
        lambdas (np.ndarray): Ridge penalty chosen for every input disease
    """

    scores: np.ndarray
    held_out_scores: np.ndarray
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
        held_out_correlation (dict[str, float]): Pearson correlation of the scores with the
            target over the held-out genes allowed in the fit, for each chromosome; NaN when
            it cannot be computed
    """

    scores: np.ndarray
    lambdas: dict[str, float]
    held_out_correlation: dict[str, float]


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

    The covariates are projected out of the target only; the features are not adjusted. By
    Frisch-Waugh-Lovell an exact joint model of target, covariates and features would project
    the covariates out of `X` too, so the scores are close to, but not the same as, the
    features' prediction adjusted for the covariates.

    The column means and standard deviations that standardise `X` are those the caller
    chooses; the predicted pleiotropy prior takes them over all genes, the held-out
    chromosome and the genes kept out of the fit included. That uses no target, so no label
    leaks, but the standardisation is transductive.

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
        matrix: scipy.sparse.spmatrix | scipy.sparse.sparray | np.ndarray,
        chunk_size: int = 500,
    ) -> Iterator[np.ndarray]:
        """Split a `genes x features` matrix into dense column chunks.

        Args:
            matrix (scipy.sparse.spmatrix | scipy.sparse.sparray | np.ndarray): Feature matrix
            chunk_size (int): Number of columns per chunk

        Yields:
            np.ndarray: Dense `genes x <=chunk_size` chunk
        """
        if isinstance(matrix, np.ndarray):
            for start in range(0, matrix.shape[1], chunk_size):
                yield matrix[:, start : start + chunk_size]
            return
        columns = scipy.sparse.csc_array(matrix)
        for start in range(0, columns.shape[1], chunk_size):
            yield columns[:, start : start + chunk_size].toarray()

    @staticmethod
    def non_constant(mean: np.ndarray, sd: np.ndarray) -> np.ndarray:
        """Which columns vary, with a tolerance relative to their mean.

        A column of a repeated float such as 0.1 has a standard deviation of about 1e-17
        rather than 0, so a plain `sd > 0` would keep it and standardise it to a column of
        ones.

        Args:
            mean (np.ndarray): Column means
            sd (np.ndarray): Column standard deviations

        Returns:
            np.ndarray: Boolean mask of the columns to keep
        """
        return sd > 1e-12 * np.maximum(1.0, np.abs(mean))

    @classmethod
    def standardise(
        cls: type[PleiotropyPrior],
        matrix: np.ndarray,
        reference_rows: np.ndarray | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Centre and scale every column to mean 0 and variance 1, dropping constant columns.

        Args:
            matrix (np.ndarray): `genes x features` matrix
            reference_rows (np.ndarray | None): Rows the means and standard deviations are taken
                over, applied to every row. Defaults to every row.

        Returns:
            tuple[np.ndarray, np.ndarray]: Standardised matrix without the columns that are
                constant over the reference rows, and the mask of the columns kept
        """
        reference = matrix if reference_rows is None else matrix[reference_rows]
        mean = reference.mean(axis=0)
        sd = reference.std(axis=0)
        keep = cls.non_constant(mean, sd)
        return (matrix[:, keep] - mean[keep]) / sd[keep], keep

    @classmethod
    def varying_columns(
        cls: type[PleiotropyPrior],
        matrix: scipy.sparse.spmatrix | scipy.sparse.sparray | np.ndarray,
        reference_rows: np.ndarray | None = None,
        chunk_size: int = 500,
    ) -> np.ndarray:
        """Columns `standardise` keeps, by the same test.

        Args:
            matrix (scipy.sparse.spmatrix | scipy.sparse.sparray | np.ndarray): Feature matrix
            reference_rows (np.ndarray | None): Rows the test is run over. Defaults to every row.
            chunk_size (int): Number of columns per chunk

        Returns:
            np.ndarray: Boolean mask over the columns of `matrix`
        """
        masks = []
        for chunk in cls.column_chunks(matrix, chunk_size):
            reference = chunk if reference_rows is None else chunk[reference_rows]
            masks.append(
                cls.non_constant(reference.mean(axis=0), reference.std(axis=0))
            )
        return np.concatenate(masks) if masks else np.zeros(0, dtype=bool)

    @classmethod
    def accumulate_kernel(
        cls: type[PleiotropyPrior],
        matrix: scipy.sparse.spmatrix | scipy.sparse.sparray | np.ndarray,
        *,
        column_weights: np.ndarray | None = None,
        reference_rows: np.ndarray | None = None,
        chunk_size: int = 500,
    ) -> tuple[np.ndarray, int]:
        """Accumulate the linear kernel `K = X X'` of the standardised features over column chunks.

        `K` is the Gram matrix of the dual ridge solve: predictions are `K alpha`, never
        `X beta`, so the dense standardised `X` is never needed as a whole and is dropped chunk
        by chunk. Each chunk is standardised on its own, which is the same as standardising `X`
        as a whole because standardisation acts column by column. The kernel is updated in
        place with a symmetric rank-k update, which fills one triangle, and mirrored once at
        the end.

        Args:
            matrix (scipy.sparse.spmatrix | scipy.sparse.sparray | np.ndarray): `genes x
                features` matrix, not standardised
            column_weights (np.ndarray | None): Weight of every column of `matrix`, applied
                after standardisation. Defaults to 1 for every column.
            reference_rows (np.ndarray | None): Rows the means and standard deviations are taken
                over. Defaults to every row.
            chunk_size (int): Number of columns per chunk

        Returns:
            tuple[np.ndarray, int]: The `genes x genes` kernel and the number of features
                that entered it

        Raises:
            ValueError: If the weights do not match the columns or no column has any variance
        """
        if column_weights is not None and column_weights.shape[0] != matrix.shape[1]:
            raise ValueError(
                f"{column_weights.shape[0]} column weights for {matrix.shape[1]} columns."
            )
        n_genes = matrix.shape[0]
        kernel: np.ndarray | None = None
        n_features = 0
        start = 0
        for chunk in cls.column_chunks(matrix, chunk_size):
            width = chunk.shape[1]
            standardised, keep = cls.standardise(chunk, reference_rows)
            if column_weights is not None:
                standardised = (
                    standardised * column_weights[start : start + width][keep]
                )
            start += width
            if standardised.shape[1] == 0:
                continue
            n_features += standardised.shape[1]
            if kernel is None:
                kernel = np.zeros((n_genes, n_genes), order="F")
            # syrk with trans=1 computes A' A; the transpose of the C-ordered chunk is
            # Fortran-ordered, so no copy is made, and only the upper triangle is written.
            kernel = scipy.linalg.blas.dsyrk(
                1.0,
                np.asarray(standardised, dtype=np.float64).T,
                beta=1.0,
                c=kernel,
                trans=1,
                overwrite_c=True,
            )
        if kernel is None:
            raise ValueError("The feature matrix has no column with any variance.")
        block = 2_048
        for low in range(0, n_genes, block):
            high = min(low + block, n_genes)
            kernel[low:high, :low] = kernel[:low, low:high].T
            diagonal = kernel[low:high, low:high]
            kernel[low:high, low:high] = np.triu(diagonal) + np.triu(diagonal, 1).T
        return kernel, n_features

    @staticmethod
    def residualise(y: np.ndarray, covariates: np.ndarray | None) -> np.ndarray:
        """Project an intercept and the covariates out of a target.

        Only the target is adjusted; the features the target is later regressed on are not.

        Args:
            y (np.ndarray): Target, one value per gene, or `genes x targets`
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
        eigenvalues: np.ndarray,
        projected_target: np.ndarray,
        lambdas: np.ndarray,
        *,
        n_samples: int | None = None,
        outside: float | np.ndarray = 0.0,
    ) -> np.ndarray:
        """Generalised cross-validation criterion of a kernel ridge fit for a grid of penalties.

        With `w_i = s_i / (s_i + lambda)` the fraction of component `i` the fit keeps:

            GCV(lambda) = n (outside + sum_i (1 - w_i)^2 u_i^2) / (n - sum_i w_i)^2

        where `outside` is the squared norm of the target outside the components, 0 for a
        full eigendecomposition.

        Args:
            eigenvalues (np.ndarray): Eigenvalues `s` of the components
            projected_target (np.ndarray): Target in the eigenbasis, `u = U' r`, one column per
                target when there are several
            lambdas (np.ndarray): Penalties to evaluate
            n_samples (int | None): Number of genes `n`. Defaults to the number of eigenvalues.
            outside (float | np.ndarray): Squared norm of each target outside the components

        Returns:
            np.ndarray: `GCV(lambda)` for every penalty in the grid, `penalties x targets` when
                there are several targets
        """
        n = eigenvalues.shape[0] if n_samples is None else n_samples
        # Clip the tiny negative eigenvalues that rounding leaves on a rank-deficient kernel.
        s = np.clip(eigenvalues, 0.0, None)[None, :]
        kept_fraction = s / (s + lambdas[:, None])
        rss = outside + ((1 - kept_fraction) ** 2) @ projected_target**2
        degrees_of_freedom = kept_fraction.sum(axis=1)
        if projected_target.ndim > 1:
            degrees_of_freedom = degrees_of_freedom[:, None]
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

        The kernel is used in single precision, so a double-precision kernel is copied once;
        pass it in single precision to avoid the copy.

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
        kernel = np.asarray(kernel, dtype=np.float32)

        scores = np.zeros(n)
        chosen: dict[str, float] = {}
        correlation: dict[str, float] = {}
        for chromosome in sorted(set(chromosomes.tolist())):
            held_out = chromosomes == chromosome
            train = ~held_out & fit_mask
            if not train.any():
                continue
            r_train = cls.residualise(
                y[train], None if covariates is None else covariates[train]
            )
            # Divide-and-conquer in single precision is about 2.5x faster than in double and
            # about 12x faster than the MRRR driver. The eigenvectors stay in single precision,
            # which halves the memory of each fold; the penalty search runs in double.
            eigenvalues, eigenvectors = scipy.linalg.eigh(
                kernel[np.ix_(train, train)],
                overwrite_a=True,
                check_finite=False,
                driver="evd",
            )
            eigenvalues = eigenvalues.astype(np.float64)
            projected = (eigenvectors.T @ r_train.astype(np.float32)).astype(np.float64)
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
            ).astype(np.float32)
            del eigenvectors
            scores[held_out] = kernel[np.ix_(held_out, train)] @ weights
            chosen[str(chromosome)] = float(grid[best])
            evaluated = held_out & fit_mask
            correlation[str(chromosome)] = (
                float(np.corrcoef(scores[evaluated], y[evaluated])[0, 1])
                if evaluated.sum() > 2
                and scores[evaluated].std() > 0
                and y[evaluated].std() > 0
                else float("nan")
            )
        return PleiotropyPriorFit(
            scores=scores, lambdas=chosen, held_out_correlation=correlation
        )

    @staticmethod
    def top_components(
        kernel: np.ndarray, n_components: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Leading eigenvalues and eigenvectors of a kernel, largest first.

        The eigendecomposition runs in single precision and computes only the leading
        components, which skips most of the back-transformation a full decomposition needs.

        Args:
            kernel (np.ndarray): `genes x genes` linear kernel
            n_components (int): Number of components to keep

        Returns:
            tuple[np.ndarray, np.ndarray]: Eigenvalues `s` and `genes x n_components`
                eigenvectors `U`, in double precision
        """
        n = kernel.shape[0]
        n_components = min(n_components, n)
        eigenvalues, eigenvectors = scipy.linalg.eigh(
            np.array(kernel, dtype=np.float32),
            overwrite_a=True,
            check_finite=False,
            subset_by_index=[n - n_components, n - 1],
            driver="evr",
        )
        order = np.argsort(eigenvalues)[::-1]
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
            tuple[np.ndarray, np.ndarray]: AUC and its 95% lower bound for every column, NaN
                for a column without both labels
        """
        ranks = scipy.stats.rankdata(scores, axis=0)
        n_pos = labels.sum(axis=0)
        n_neg = labels.shape[0] - n_pos
        # A column with no positive or no negative gene has no AUC: it comes back as NaN.
        with np.errstate(divide="ignore", invalid="ignore"):
            auc = ((ranks * labels).sum(axis=0) - n_pos * (n_pos + 1) / 2) / (
                n_pos * n_neg
            )
            q1 = auc / (2 - auc)
            q2 = 2 * auc**2 / (1 + auc)
            variance = (
                auc * (1 - auc)
                + (n_pos - 1) * (q1 - auc**2)
                + (n_neg - 1) * (q2 - auc**2)
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
        held_out_kernel: np.ndarray | None = None,
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
            score_h = K_hT U diag(1 / (s + lambda)) u   gene h kept out of the fit

        `loo_g` is exactly the score gene `g` gets from the same ridge fitted without `g` on the
        residuals `r`. The covariates are projected out once over all genes, so a gene's own
        label still reaches its score through that projection; with about a dozen covariates
        and 20,000 genes its average weight is below 0.1%. `lambda` minimises generalised
        cross-validation, `n RSS / (n - sum_i w_i)^2` with `RSS = |r|^2 - |u|^2 +
        sum_i (1 - w_i)^2 u_i^2`.

        A gene `h` kept out of the fit, such as a gene in the MHC, is scored with the same
        ridge through its kernel row `K_hT` against the fitted genes: its projection on the
        components, times the ridge coefficients. For a fitted gene the same formula gives
        `fitted_g`.

        A disease is kept when the 95% lower bound of the AUC of its leave-one-out scores is
        above 0.5, i.e. when the gene features predict its nearest genes better than chance.
        The scores of a kept disease are then z-scored across the fitted genes, so that
        diseases with different numbers of nearest genes are on the same scale; the scores of
        the genes kept out of the fit take the same mean and standard deviation. A disease
        whose scores do not vary is dropped.

        Args:
            eigenvalues (np.ndarray): Eigenvalues `s` of the top components
            eigenvectors (np.ndarray): `genes x k` eigenvectors `U`
            labels (np.ndarray): `genes x diseases` 0/1 matrix, 1 when the gene is a nearest
                gene of the disease
            covariates (np.ndarray | None): `genes x covariates` matrix projected out of the
                labels, without intercept
            lambdas (Iterable[float]): Ridge penalties searched by generalised cross-validation
            held_out_kernel (np.ndarray | None): `held-out genes x genes` kernel rows of the
                genes kept out of the fit against the fitted genes, built with the same
                standardisation and weights

        Returns:
            ApproxPopsFit: Scores of the kept diseases and the diagnostics of every disease
        """
        grid = np.asarray(sorted(lambdas), dtype=np.float64)
        n = labels.shape[0]
        s = np.clip(eigenvalues, 0.0, None)
        residuals = cls.residualise(labels, covariates)
        projected = eigenvectors.T @ residuals
        outside = (residuals**2).sum(axis=0) - (projected**2).sum(axis=0)
        gcv = cls.generalised_cross_validation(
            s, projected, grid, n_samples=n, outside=outside
        )
        best = gcv.argmin(axis=0)
        penalty = grid[best][None, :]
        kept_fraction = s[:, None] / (s[:, None] + penalty)
        fitted = eigenvectors @ (kept_fraction * projected)
        leverage = eigenvectors**2 @ kept_fraction
        loo = (fitted - leverage * residuals) / (1 - leverage)
        auc, lower = cls.auc_with_lower_bound(loo, labels)
        mean = loo.mean(axis=0)
        sd = loo.std(axis=0)
        kept = (lower > 0.5) & cls.non_constant(mean, sd)
        scores = (loo[:, kept] - mean[kept]) / sd[kept]
        if held_out_kernel is None:
            held_out_scores = np.zeros((0, int(kept.sum())))
        else:
            coefficients = eigenvectors @ (projected / (s[:, None] + penalty))
            held_out_scores = (
                np.asarray(held_out_kernel, dtype=np.float64) @ coefficients[:, kept]
                - mean[kept]
            ) / sd[kept]
        return ApproxPopsFit(
            scores=scores,
            held_out_scores=held_out_scores,
            kept=kept,
            auc=auc,
            auc_lower_bound=lower,
            lambdas=grid[best],
        )
