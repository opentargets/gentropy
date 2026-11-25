"""Tests for the leave-one-chromosome-out kernel ridge regression of the pleiotropy prior."""

from __future__ import annotations

import numpy as np
import pytest
import scipy.sparse
from sklearn.kernel_ridge import KernelRidge
from sklearn.linear_model import Ridge
from sklearn.metrics import roc_auc_score

from gentropy.method.pleiotropy_prior import PleiotropyPrior


@pytest.fixture()
def toy_genes() -> dict[str, np.ndarray]:
    """Fifty genes on three chromosomes with twenty features, two covariates and a target."""
    rng = np.random.default_rng(42)
    n_genes, n_features = 50, 20
    features, _ = PleiotropyPrior.standardise(rng.normal(size=(n_genes, n_features)))
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
        assert fit.penalty == penalty
        assert set(fit.held_out_correlation) == {"1", "2", "3"}

    @pytest.mark.parametrize("adjusted", [False, True])
    def test_kernel_ridge_equals_sklearn_kernel_ridge(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray], adjusted: bool
    ) -> None:
        """The dual solve on the precomputed kernel matches sklearn's KernelRidge.

        With `adjusted`, the covariates are projected out within the fold and one gene of the
        training chromosomes is kept out of the fit.
        """
        penalty = 3.0
        kernel, chromosomes = toy_genes["kernel"], toy_genes["chromosomes"]
        covariates = toy_genes["covariates"] if adjusted else None
        fit_mask = np.ones(50, dtype=bool)
        if adjusted:
            fit_mask[30] = False
        fit = PleiotropyPrior.loco_kernel_ridge(
            kernel,
            toy_genes["y"],
            chromosomes,
            covariates=covariates,
            fit_mask=fit_mask,
            lambdas=[penalty],
        )
        held_out = chromosomes == "1"
        train = ~held_out & fit_mask
        target = PleiotropyPrior.residualise(
            toy_genes["y"][train], None if covariates is None else covariates[train]
        )
        reference = KernelRidge(alpha=penalty, kernel="precomputed").fit(
            kernel[np.ix_(train, train)], target
        )
        np.testing.assert_allclose(
            fit.scores[held_out],
            reference.predict(kernel[np.ix_(held_out, train)]),
            rtol=1e-4,
            atol=1e-5,
        )

    def test_one_penalty_chosen_over_the_fitted_genes(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """The penalty minimises GCV over the genes in the fit and serves every chromosome."""
        kernel, y = toy_genes["kernel"], toy_genes["y"]
        covariates = toy_genes["covariates"]
        fit_mask = np.ones(50, dtype=bool)
        fit_mask[30] = False
        lambdas = np.logspace(-2, 4, 13)
        fit = PleiotropyPrior.loco_kernel_ridge(
            kernel,
            y,
            toy_genes["chromosomes"],
            covariates=covariates,
            fit_mask=fit_mask,
            lambdas=lambdas,
        )
        target = PleiotropyPrior.residualise(y[fit_mask], covariates[fit_mask])
        eigenvalues, eigenvectors = np.linalg.eigh(kernel[np.ix_(fit_mask, fit_mask)])
        gcv = PleiotropyPrior.generalised_cross_validation(
            eigenvalues, eigenvectors.T @ target, lambdas
        )
        assert fit.penalty == pytest.approx(lambdas[int(np.argmin(gcv))])
        single = PleiotropyPrior.loco_kernel_ridge(
            kernel,
            y,
            toy_genes["chromosomes"],
            covariates=covariates,
            fit_mask=fit_mask,
            lambdas=[fit.penalty],
        )
        np.testing.assert_allclose(fit.scores, single.scores)

    def test_score_does_not_depend_on_own_target(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """At a given penalty, a gene's target leaves the scores of its chromosome unchanged.

        The penalty is chosen over every gene in the fit, so it is fixed here.
        """
        before = PleiotropyPrior.loco_kernel_ridge(
            toy_genes["kernel"],
            toy_genes["y"],
            toy_genes["chromosomes"],
            covariates=toy_genes["covariates"],
            lambdas=[3.0],
        )
        y = toy_genes["y"].copy()
        y[5] += 100.0
        after = PleiotropyPrior.loco_kernel_ridge(
            toy_genes["kernel"],
            y,
            toy_genes["chromosomes"],
            covariates=toy_genes["covariates"],
            lambdas=[3.0],
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

    def test_generalised_cross_validation_of_several_targets(
        self: TestPleiotropyPrior, toy_genes: dict[str, np.ndarray]
    ) -> None:
        """Several targets at once give the same criterion as one target at a time."""
        eigenvalues, eigenvectors = np.linalg.eigh(toy_genes["kernel"])
        targets = PleiotropyPrior.residualise(
            np.column_stack([toy_genes["y"], toy_genes["covariates"][:, 0]]), None
        )
        lambdas = np.logspace(-2, 4, 7)
        outside = np.array([0.5, 2.0])
        together = PleiotropyPrior.generalised_cross_validation(
            eigenvalues,
            eigenvectors.T @ targets,
            lambdas,
            n_samples=60,
            outside=outside,
        )
        for column in range(2):
            np.testing.assert_allclose(
                together[:, column],
                PleiotropyPrior.generalised_cross_validation(
                    eigenvalues,
                    eigenvectors.T @ targets[:, column],
                    lambdas,
                    n_samples=60,
                    outside=outside[column],
                ),
            )

    def test_generalised_cross_validation_picks_known_minimum(
        self: TestPleiotropyPrior,
    ) -> None:
        """Pure noise is best fitted with the largest penalty, a clean signal with a small one."""
        rng = np.random.default_rng(0)
        features, _ = PleiotropyPrior.standardise(rng.normal(size=(200, 5)))
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
        kernel, n_features = PleiotropyPrior.accumulate_kernel(features, chunk_size=10)
        standardised, keep = PleiotropyPrior.standardise(features)
        np.testing.assert_allclose(kernel, standardised @ standardised.T, atol=1e-10)
        assert n_features == 22
        assert not keep[4]

    def test_kernel_standardised_on_reference_rows(
        self: TestPleiotropyPrior,
    ) -> None:
        """Means and standard deviations come from the reference rows and apply to every row."""
        rng = np.random.default_rng(3)
        features = rng.normal(size=(30, 8))
        reference = np.arange(30) < 25
        kernel, _ = PleiotropyPrior.accumulate_kernel(
            features, reference_rows=reference, chunk_size=3
        )
        expected = (features - features[reference].mean(axis=0)) / features[
            reference
        ].std(axis=0)
        np.testing.assert_allclose(kernel, expected @ expected.T, atol=1e-10)

    def test_repeated_float_column_is_constant(self: TestPleiotropyPrior) -> None:
        """A column of a repeated float is dropped although its sd is not exactly 0."""
        features = np.column_stack([np.full(20_107, 0.1), np.arange(20_107.0)])
        assert features[:, 0].std() > 0
        standardised, keep = PleiotropyPrior.standardise(features)
        assert keep.tolist() == [False, True]
        assert standardised.shape == (20_107, 1)
        assert PleiotropyPrior.varying_columns(features).tolist() == [False, True]

    def test_column_weights_must_match_the_columns(
        self: TestPleiotropyPrior,
    ) -> None:
        """A weight vector of the wrong length is refused."""
        features = np.random.default_rng(4).normal(size=(10, 5))
        with pytest.raises(ValueError, match="column weights"):
            PleiotropyPrior.accumulate_kernel(features, column_weights=np.ones(6))

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
        features, _ = PleiotropyPrior.standardise(rng.normal(size=(60, 10)))
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
        cap = PleiotropyPrior.z_cap(60)
        expected = np.clip((refitted - refitted.mean()) / refitted.std(), -cap, cap)
        np.testing.assert_allclose(fit.scores[:, 0], expected, atol=1e-4)

    def test_noise_disease_is_dropped_and_scores_are_standardised(
        self: TestApproxPops, toy: dict[str, np.ndarray]
    ) -> None:
        """Only the predictable disease passes the AUC filter; its z-scores are clipped."""
        fit = PleiotropyPrior.approx_pops(
            toy["eigenvalues"], toy["eigenvectors"], toy["labels"]
        )
        assert fit.kept.tolist() == [True, False]
        assert fit.scores.shape == (60, 1)
        assert fit.auc_lower_bound[1] <= 0.5
        assert np.abs(fit.scores).max() <= PleiotropyPrior.z_cap(60)
        unclipped = np.abs(fit.scores) < PleiotropyPrior.z_cap(60)
        assert unclipped.mean() > 0.9
        assert fit.held_out_scores.shape == (0, 1)

    def test_z_cap(self: TestApproxPops) -> None:
        """The cap is sqrt(2 ln n), about 4.45 for 20,000 genes."""
        assert PleiotropyPrior.z_cap(20_000) == pytest.approx(4.45, abs=0.01)

    def test_genes_out_of_the_fit_get_the_fitted_ridge_prediction(
        self: TestApproxPops, toy: dict[str, np.ndarray]
    ) -> None:
        """Scored through their kernel rows, the fitted genes get their in-sample fit back."""
        penalty = 2.0
        kernel = toy["features"] @ toy["features"].T
        fit = PleiotropyPrior.approx_pops(
            toy["eigenvalues"],
            toy["eigenvectors"],
            toy["labels"],
            lambdas=[penalty],
            held_out_kernel=kernel[:7],
        )
        components = toy["eigenvectors"] * np.sqrt(toy["eigenvalues"])
        residual = PleiotropyPrior.residualise(toy["labels"][:, 0], None)
        in_sample = (
            Ridge(alpha=penalty, fit_intercept=False)
            .fit(components, residual)
            .predict(components[:7])
        )
        # Held-out scores take the mean and sd of the leave-one-out scores of the fitted genes.
        refitted_loo = np.array(
            [
                Ridge(alpha=penalty, fit_intercept=False)
                .fit(np.delete(components, g, axis=0), np.delete(residual, g))
                .predict(components[[g]])[0]
                for g in range(60)
            ]
        )
        cap = PleiotropyPrior.z_cap(60)
        np.testing.assert_allclose(
            fit.held_out_scores[:, 0],
            np.clip((in_sample - refitted_loo.mean()) / refitted_loo.std(), -cap, cap),
            atol=1e-4,
        )

    def test_auc_is_nan_without_both_labels(self: TestApproxPops) -> None:
        """A column with no positive gene has no AUC rather than a division error."""
        labels = np.zeros((10, 1))
        auc, lower = PleiotropyPrior.auc_with_lower_bound(
            np.arange(10.0)[:, None], labels
        )
        assert np.isnan(auc[0])
        assert np.isnan(lower[0])

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
            scipy.sparse.csc_array(matrix), column_weights=weights, chunk_size=4
        )
        keep = [0, 1, 3, 4, 5]
        weighted = PleiotropyPrior.standardise(matrix)[0] * weights[keep]
        np.testing.assert_allclose(kernel, weighted @ weighted.T)
        assert n_features == 5
