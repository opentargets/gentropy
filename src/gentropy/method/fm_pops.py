"""Gene-level prior from the PoPS gene features, trained on fine-mapped GWAS results.

The model follows the polygenic priority score (PoPS; Weeks et al. 2023, Nat Genet
55:1267-1276): a ridge regression of a gene-level target on gene features, fitted without the
chromosome of the gene being scored. PoPS regresses MAGMA gene z-scores, which need full
summary statistics. Here the target is computed from credible sets alone, so the prior can be
built for every study in a release.

This module is written from the formulas in the documentation of
[`FmPops`][gentropy.method.fm_pops.FmPops]; it does not reuse the PoPS code, which is released
under GPL-3.0.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import scipy.linalg

logger = logging.getLogger(__name__)


@dataclass
class FmPopsFit:
    """Result of the leave-one-chromosome-out kernel ridge regression.

    Attributes:
        scores (np.ndarray): Out-of-chromosome prediction for every gene, in the order of the
            input
        lambdas (dict[str, float]): Ridge penalty chosen by generalised cross-validation for
            each held-out chromosome
    """

    scores: np.ndarray
    lambdas: dict[str, float]


class FmPops:
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

    The kernel form never holds the `n x p` feature matrix in memory: `K` is accumulated over
    column chunks, so the cost is set by the number of genes (about 18,000), not by the number
    of features (about 57,000).
    """

    DEFAULT_LAMBDA_GRID: tuple[float, ...] = tuple(
        float(v) for v in np.logspace(-2, 10, 25)
    )
    """Ridge penalties searched by default, `10^-2` to `10^10` in steps of `10^0.5`."""

    MATRIX_DIR = "munged_features"
    MATRIX_PREFIX = "pops_features"

    @classmethod
    def read_feature_genes(cls: type[FmPops], feature_dir: str | Path) -> list[str]:
        """Read the genes the PoPS feature matrices hold rows for, in row order.

        Args:
            feature_dir (str | Path): Directory of the extracted PoPS features

        Returns:
            list[str]: Ensembl gene identifiers
        """
        rows = Path(feature_dir) / cls.MATRIX_DIR / f"{cls.MATRIX_PREFIX}.rows.txt"
        return [line.strip() for line in rows.read_text().splitlines() if line.strip()]

    @staticmethod
    def read_control_feature_names(feature_dir: str | Path) -> set[str]:
        """Read the names of the PoPS control features.

        Args:
            feature_dir (str | Path): Directory of the extracted PoPS features

        Returns:
            set[str]: Names of the control columns
        """
        path = Path(feature_dir) / "control.features"
        return {line.strip() for line in path.read_text().splitlines() if line.strip()}

    @classmethod
    def read_feature_chunks(
        cls: type[FmPops], feature_dir: str | Path, gene_ids: list[str]
    ) -> Iterator[tuple[list[str], np.ndarray]]:
        """Read the PoPS feature matrices chunk by chunk, restricted to a list of genes.

        The PoPS features ship as `munged_features/pops_features.mat.{i}.npy`, each a
        `genes x <=500` float64 matrix whose column names are in the matching `.cols.{i}.txt`
        file and whose rows are listed once in `pops_features.rows.txt`.

        Args:
            feature_dir (str | Path): Directory of the extracted PoPS features
            gene_ids (list[str]): Genes to keep, in the row order wanted in the output. Every
                one must be present in the feature rows.

        Yields:
            tuple[list[str], np.ndarray]: Column names and the `len(gene_ids) x columns` chunk

        Raises:
            FileNotFoundError: If the directory holds no feature matrix
            ValueError: If a requested gene has no row in the feature matrices
        """
        matrix_dir = Path(feature_dir) / cls.MATRIX_DIR
        row_of = {gene: i for i, gene in enumerate(cls.read_feature_genes(feature_dir))}
        missing = [gene for gene in gene_ids if gene not in row_of]
        if missing:
            raise ValueError(
                f"{len(missing)} genes have no row in the PoPS features, e.g. {missing[:3]}."
            )
        rows = np.array([row_of[gene] for gene in gene_ids], dtype=np.int64)

        chunk_ids = sorted(
            int(path.name.split(".")[-2])
            for path in matrix_dir.glob(f"{cls.MATRIX_PREFIX}.mat.*.npy")
        )
        if not chunk_ids:
            raise FileNotFoundError(f"No PoPS feature matrix found in {matrix_dir}.")
        for i in chunk_ids:
            columns = (
                (matrix_dir / f"{cls.MATRIX_PREFIX}.cols.{i}.txt").read_text().split()
            )
            matrix = np.load(matrix_dir / f"{cls.MATRIX_PREFIX}.mat.{i}.npy")
            yield columns, np.ascontiguousarray(matrix[rows], dtype=np.float64)

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
        cls: type[FmPops], chunks: Iterable[np.ndarray]
    ) -> tuple[np.ndarray, int]:
        """Accumulate the linear kernel `K = X X'` over column chunks of `X`.

        Each chunk is standardised on its own, which is the same as standardising `X` as a
        whole because standardisation acts column by column.

        Args:
            chunks (Iterable[np.ndarray]): `genes x columns` chunks of the feature matrix, all
                with the same rows

        Returns:
            tuple[np.ndarray, int]: The `genes x genes` kernel and the number of features
                that entered it

        Raises:
            ValueError: If no chunk holds a non-constant column
        """
        kernel: np.ndarray | None = None
        n_features = 0
        for chunk in chunks:
            standardised = cls.standardise(chunk)
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
        cls: type[FmPops],
        kernel: np.ndarray,
        y: np.ndarray,
        chromosomes: np.ndarray,
        *,
        covariates: np.ndarray | None = None,
        fit_mask: np.ndarray | None = None,
        lambdas: Iterable[float] = DEFAULT_LAMBDA_GRID,
    ) -> FmPopsFit:
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
            FmPopsFit: Scores and the penalty chosen for every chromosome

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
            # The MRRR driver needs O(n) workspace; the divide-and-conquer one needs another
            # 2 n^2 doubles, which is about 5 GB at 18,000 genes.
            eigenvalues, eigenvectors = scipy.linalg.eigh(
                kernel[np.ix_(train, train)],
                overwrite_a=True,
                check_finite=False,
                driver="evr",
            )
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
        return FmPopsFit(scores=scores, lambdas=chosen)
