"""Tests for the fmPops leave-one-chromosome-out kernel ridge regression."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from sklearn.linear_model import Ridge

from gentropy.method.fm_pops import FmPops


@pytest.fixture()
def toy_genes() -> dict[str, np.ndarray]:
    """Fifty genes on three chromosomes with twenty features, two covariates and a target."""
    rng = np.random.default_rng(42)
    n_genes, n_features = 50, 20
    features = FmPops.standardise(rng.normal(size=(n_genes, n_features)))
    covariates = rng.normal(size=(n_genes, 2))
    y = features[:, :3] @ np.array([1.0, -0.5, 0.25]) + rng.normal(size=n_genes)
    chromosomes = np.array(["1"] * 20 + ["2"] * 18 + ["3"] * 12)
    return {
        "features": features,
        "kernel": features @ features.T,
        "covariates": covariates,
        "y": y,
        "chromosomes": chromosomes,
    }


class TestFmPops:
    """Test the numerical parts of fmPops."""

    def test_kernel_ridge_equals_primal_ridge(
        self: TestFmPops, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """The dual solution scores the held-out genes as a primal ridge fit would."""
        penalty = 3.0
        fit = FmPops.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            lambdas=[penalty],
        )
        held_out = toy_genes["chromosomes"] == "1"
        target = FmPops.residualise(toy_genes["y"][~held_out], None)
        primal = Ridge(alpha=penalty, fit_intercept=False, solver="cholesky").fit(
            toy_genes["features"][~held_out], target
        )
        np.testing.assert_allclose(
            fit.scores[held_out],
            primal.predict(toy_genes["features"][held_out]),
            atol=1e-8,
        )
        assert fit.lambdas == {"1": penalty, "2": penalty, "3": penalty}

    def test_score_does_not_depend_on_own_target(
        self: TestFmPops, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """Changing the target of a gene leaves the scores of its chromosome unchanged."""
        before = FmPops.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            covariates=toy_genes["covariates"],
        )
        y = toy_genes["y"].copy()
        y[5] += 100.0
        after = FmPops.loco_kernel_ridge(
            toy_genes["kernel"],
            y,
            toy_genes["chromosomes"],
            covariates=toy_genes["covariates"],
        )
        same_chromosome = toy_genes["chromosomes"] == toy_genes["chromosomes"][5]
        np.testing.assert_allclose(
            after.scores[same_chromosome], before.scores[same_chromosome]
        )
        assert not np.allclose(
            after.scores[~same_chromosome], before.scores[~same_chromosome]
        )

    def test_genes_outside_fit_mask_are_scored_but_not_fitted(
        self: TestFmPops, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """A gene kept out of the fit still gets a score and its target changes nothing."""
        fit_mask = np.ones(50, dtype=bool)
        fit_mask[30] = False
        before = FmPops.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            fit_mask=fit_mask,
        )
        y = toy_genes["y"].copy()
        y[30] += 100.0
        after = FmPops.loco_kernel_ridge(
            toy_genes["kernel"], y, toy_genes["chromosomes"], fit_mask=fit_mask
        )
        np.testing.assert_allclose(after.scores, before.scores)
        assert before.scores[30] != 0.0

    def test_generalised_cross_validation_matches_hat_matrix(
        self: TestFmPops, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """GCV from the eigendecomposition equals GCV from the explicit hat matrix."""
        kernel = toy_genes["kernel"]
        target = FmPops.residualise(toy_genes["y"], None)
        n = target.shape[0]
        lambdas = np.logspace(-2, 4, 13)
        eigenvalues, eigenvectors = np.linalg.eigh(kernel)
        observed = FmPops.generalised_cross_validation(
            eigenvalues, eigenvectors.T @ target, lambdas
        )
        expected = []
        for penalty in lambdas:
            residual_maker = np.eye(n) - kernel @ np.linalg.inv(
                kernel + penalty * np.eye(n)
            )
            expected.append(
                n
                * np.sum((residual_maker @ target) ** 2)
                / np.trace(residual_maker) ** 2
            )
        np.testing.assert_allclose(observed, expected, rtol=1e-8)
        assert int(np.argmin(observed)) == int(np.argmin(expected))

    def test_generalised_cross_validation_picks_known_minimum(
        self: TestFmPops,
    ) -> None:
        """Pure noise is best fitted with the largest penalty, a clean signal with a small one."""
        rng = np.random.default_rng(0)
        features = FmPops.standardise(rng.normal(size=(200, 5)))
        kernel = features @ features.T
        eigenvalues, eigenvectors = np.linalg.eigh(kernel)
        lambdas = np.logspace(-2, 6, 17)

        noise = FmPops.residualise(rng.normal(size=200), None)
        gcv_noise = FmPops.generalised_cross_validation(
            eigenvalues, eigenvectors.T @ noise, lambdas
        )
        assert int(np.argmin(gcv_noise)) == lambdas.size - 1

        signal = FmPops.residualise(features @ np.ones(5), None)
        gcv_signal = FmPops.generalised_cross_validation(
            eigenvalues, eigenvectors.T @ signal, lambdas
        )
        assert int(np.argmin(gcv_signal)) == 0

    def test_residualise_is_orthogonal_to_covariates(
        self: TestFmPops, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """The residual is orthogonal to the intercept and every covariate."""
        residual = FmPops.residualise(toy_genes["y"], toy_genes["covariates"])
        design = np.column_stack([np.ones(50), toy_genes["covariates"]])
        np.testing.assert_allclose(design.T @ residual, 0.0, atol=1e-10)

    def test_chunked_kernel_equals_full_kernel(
        self: TestFmPops,
    ) -> None:
        """Accumulating the kernel over column chunks gives X X' of the standardised matrix."""
        rng = np.random.default_rng(1)
        features = rng.normal(size=(30, 23))
        features[:, 4] = 7.0
        chunks = [features[:, :10], features[:, 10:20], features[:, 20:]]
        kernel, n_features = FmPops.accumulate_kernel(chunks)
        standardised = FmPops.standardise(features)
        np.testing.assert_allclose(kernel, standardised @ standardised.T, atol=1e-10)
        assert n_features == 22

    def test_read_feature_chunks(self: TestFmPops, tmp_path: Path) -> None:
        """Chunks are read in order and restricted to the requested genes, in their order."""
        matrix_dir = tmp_path / FmPops.MATRIX_DIR
        matrix_dir.mkdir()
        (matrix_dir / "pops_features.rows.txt").write_text("g1\ng2\ng3\n")
        (tmp_path / "control.features").write_text("a.control\n")
        for i, columns in enumerate([["a.1", "a.control"], ["b.1"]]):
            (matrix_dir / f"pops_features.cols.{i}.txt").write_text(
                "\n".join(columns) + "\n"
            )
            np.save(
                matrix_dir / f"pops_features.mat.{i}.npy",
                np.arange(3 * len(columns), dtype=np.float64).reshape(3, -1) + 10 * i,
            )

        chunks = list(FmPops.read_feature_chunks(tmp_path, ["g3", "g1"]))

        assert FmPops.read_control_feature_names(tmp_path) == {"a.control"}
        assert [columns for columns, _ in chunks] == [["a.1", "a.control"], ["b.1"]]
        np.testing.assert_array_equal(chunks[0][1], [[4.0, 5.0], [0.0, 1.0]])
        np.testing.assert_array_equal(chunks[1][1], [[12.0], [10.0]])
        with pytest.raises(ValueError, match="no row"):
            list(FmPops.read_feature_chunks(tmp_path, ["g4"]))
