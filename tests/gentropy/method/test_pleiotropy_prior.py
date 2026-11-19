"""Tests for the leave-one-chromosome-out kernel ridge regression of the pleiotropy prior."""

from __future__ import annotations

import numpy as np
import pytest
import scipy.sparse
from sklearn.linear_model import Ridge
from sklearn.metrics import roc_auc_score

from gentropy.method.pleiotropy_prior import PleiotropyPrior


@pytest.fixture()
def toy_genes() -> dict[str, np.ndarray]:
    """Fifty genes on three chromosomes with twenty features, two covariates and a target."""
    rng = np.random.default_rng(42)
    n_genes, n_features = 50, 20
    features = PleiotropyPrior.standardise(rng.normal(size=(n_genes, n_features)))
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


class TestPleiotropyPrior:
    """Test the numerical parts of the pleiotropy prior."""

    def test_kernel_ridge_equals_primal_ridge(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """The dual solution scores the held-out genes as a primal ridge fit would."""
        penalty = 3.0
        fit = PleiotropyPrior.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            lambdas=[penalty],
        )
        held_out = toy_genes["chromosomes"] == "1"
        target = PleiotropyPrior.residualise(toy_genes["y"][~held_out], None)
        primal = Ridge(alpha=penalty, fit_intercept=False, solver="cholesky").fit(
            toy_genes["features"][~held_out], target
        )
        np.testing.assert_allclose(
            fit.scores[held_out],
            primal.predict(toy_genes["features"][held_out]),
            rtol=1e-4,
            atol=1e-5,
        )
        assert fit.lambdas == {"1": penalty, "2": penalty, "3": penalty}

    def test_score_does_not_depend_on_own_target(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """Changing the target of a gene leaves the scores of its chromosome unchanged."""
        before = PleiotropyPrior.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            covariates=toy_genes["covariates"],
        )
        y = toy_genes["y"].copy()
        y[5] += 100.0
        after = PleiotropyPrior.loco_kernel_ridge(
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
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """A gene kept out of the fit still gets a score and its target changes nothing."""
        fit_mask = np.ones(50, dtype=bool)
        fit_mask[30] = False
        before = PleiotropyPrior.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            fit_mask=fit_mask,
        )
        y = toy_genes["y"].copy()
        y[30] += 100.0
        after = PleiotropyPrior.loco_kernel_ridge(
            toy_genes["kernel"], y, toy_genes["chromosomes"], fit_mask=fit_mask
        )
        np.testing.assert_allclose(after.scores, before.scores)
        assert before.scores[30] != 0.0

    def test_generalised_cross_validation_matches_hat_matrix(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """GCV from the eigendecomposition equals GCV from the explicit hat matrix."""
        kernel = toy_genes["kernel"]
        target = PleiotropyPrior.residualise(toy_genes["y"], None)
        n = target.shape[0]
        lambdas = np.logspace(-2, 4, 13)
        eigenvalues, eigenvectors = np.linalg.eigh(kernel)
        observed = PleiotropyPrior.generalised_cross_validation(
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
        self: TestPleiotropyPrior,
    ) -> None:
        """Pure noise is best fitted with the largest penalty, a clean signal with a small one."""
        rng = np.random.default_rng(0)
        features = PleiotropyPrior.standardise(rng.normal(size=(200, 5)))
        kernel = features @ features.T
        eigenvalues, eigenvectors = np.linalg.eigh(kernel)
        lambdas = np.logspace(-2, 6, 17)

        noise = PleiotropyPrior.residualise(rng.normal(size=200), None)
        gcv_noise = PleiotropyPrior.generalised_cross_validation(
            eigenvalues, eigenvectors.T @ noise, lambdas
        )
        assert int(np.argmin(gcv_noise)) == lambdas.size - 1

        signal = PleiotropyPrior.residualise(features @ np.ones(5), None)
        gcv_signal = PleiotropyPrior.generalised_cross_validation(
            eigenvalues, eigenvectors.T @ signal, lambdas
        )
        assert int(np.argmin(gcv_signal)) == 0

    def test_residualise_is_orthogonal_to_covariates(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """The residual is orthogonal to the intercept and every covariate."""
        residual = PleiotropyPrior.residualise(toy_genes["y"], toy_genes["covariates"])
        design = np.column_stack([np.ones(50), toy_genes["covariates"]])
        np.testing.assert_allclose(design.T @ residual, 0.0, atol=1e-10)

    def test_chunked_kernel_equals_full_kernel(
        self: TestPleiotropyPrior,
    ) -> None:
        """Accumulating the kernel over column chunks gives X X' of the standardised matrix."""
        rng = np.random.default_rng(1)
        features = rng.normal(size=(30, 23))
        features[:, 4] = 7.0
        chunks = [features[:, :10], features[:, 10:20], features[:, 20:]]
        kernel, n_features = PleiotropyPrior.accumulate_kernel(chunks)
        standardised = PleiotropyPrior.standardise(features)
        np.testing.assert_allclose(kernel, standardised @ standardised.T, atol=1e-10)
        assert n_features == 22

    def test_column_chunks_of_sparse_matrix(self: TestPleiotropyPrior) -> None:
        """A sparse matrix comes back as dense column chunks that add up to the whole."""
        matrix = scipy.sparse.random(
            20, 23, density=0.3, random_state=np.random.default_rng(2), format="csr"
        )
        chunks = list(PleiotropyPrior.column_chunks(matrix, chunk_size=10))
        assert [chunk.shape for chunk in chunks] == [(20, 10), (20, 10), (20, 3)]
        np.testing.assert_array_equal(np.hstack(chunks), matrix.toarray())


class TestApproxPops:
    """Test the per-disease ridge regression on the top principal components."""

    @pytest.fixture()
    def toy(self: TestApproxPops) -> dict[str, np.ndarray]:
        """Sixty genes, ten features, a predictable disease and a disease of pure noise."""
        rng = np.random.default_rng(7)
        features = PleiotropyPrior.standardise(rng.normal(size=(60, 10)))
        signal = features[:, :2] @ np.array([1.0, 1.0]) + 0.5 * rng.normal(size=60)
        labels = np.zeros((60, 2))
        labels[np.argsort(-signal)[:20], 0] = 1.0
        labels[rng.choice(60, size=20, replace=False), 1] = 1.0
        eigenvalues, eigenvectors = PleiotropyPrior.top_components(
            features @ features.T, 5
        )
        return {
            "features": features,
            "labels": labels,
            "eigenvalues": eigenvalues,
            "eigenvectors": eigenvectors,
        }

    def test_top_components_are_the_largest(
        self: TestApproxPops, toy: dict[str, np.ndarray]
    ) -> None:
        """Eigenvalues come largest first and match the full decomposition."""
        kernel = toy["features"] @ toy["features"].T
        expected = np.sort(np.linalg.eigvalsh(kernel))[::-1][:5]
        np.testing.assert_allclose(toy["eigenvalues"], expected, rtol=1e-4)
        assert toy["eigenvectors"].shape == (60, 5)

    def test_leave_one_out_equals_refitting_without_the_gene(
        self: TestApproxPops, toy: dict[str, np.ndarray]
    ) -> None:
        """The closed-form score of a gene is the score of a ridge fitted without it."""
        penalty = 2.0
        fit = PleiotropyPrior.approx_pops(
            toy["eigenvalues"], toy["eigenvectors"], toy["labels"], lambdas=[penalty]
        )
        components = toy["eigenvectors"] * np.sqrt(toy["eigenvalues"])
        residual = PleiotropyPrior.residualise(toy["labels"][:, 0], None)
        refitted = np.array(
            [
                Ridge(alpha=penalty, fit_intercept=False)
                .fit(np.delete(components, g, axis=0), np.delete(residual, g))
                .predict(components[[g]])[0]
                for g in range(60)
            ]
        )
        expected = (refitted - refitted.mean()) / refitted.std()
        np.testing.assert_allclose(fit.scores[:, 0], expected, atol=1e-4)

    def test_noise_disease_is_dropped_and_scores_are_standardised(
        self: TestApproxPops, toy: dict[str, np.ndarray]
    ) -> None:
        """Only the predictable disease passes the AUC filter, and its scores are z-scores."""
        fit = PleiotropyPrior.approx_pops(
            toy["eigenvalues"], toy["eigenvectors"], toy["labels"]
        )
        assert fit.kept.tolist() == [True, False]
        assert fit.scores.shape == (60, 1)
        assert fit.auc_lower_bound[1] <= 0.5
        np.testing.assert_allclose(fit.scores.mean(axis=0), 0.0, atol=1e-10)
        np.testing.assert_allclose(fit.scores.std(axis=0), 1.0)

    def test_auc_matches_sklearn(self: TestApproxPops) -> None:
        """The rank AUC equals scikit-learn's, ties included."""
        rng = np.random.default_rng(0)
        labels = (rng.random((40, 3)) < 0.3).astype(float)
        scores = np.round(labels + rng.normal(size=(40, 3)), 1)
        auc, lower = PleiotropyPrior.auc_with_lower_bound(scores, labels)
        expected = [roc_auc_score(labels[:, d], scores[:, d]) for d in range(3)]
        np.testing.assert_allclose(auc, expected)
        assert (lower < auc).all()

    def test_weighted_kernel(self: TestApproxPops) -> None:
        """Column weights apply after standardisation and skip constant columns correctly."""
        rng = np.random.default_rng(1)
        matrix = rng.normal(size=(15, 6))
        matrix[:, 2] = 3.0
        weights = np.array([1.0, 2.0, 5.0, 0.5, 1.0, 3.0])
        kernel, n_features = PleiotropyPrior.accumulate_kernel(
            PleiotropyPrior.column_chunks(scipy.sparse.csc_array(matrix), chunk_size=4),
            column_weights=weights,
        )
        keep = [0, 1, 3, 4, 5]
        weighted = PleiotropyPrior.standardise(matrix) * weights[keep]
        np.testing.assert_allclose(kernel, weighted @ weighted.T)
        assert n_features == 5
