"""Tests for LDSC SNP-heritability regression and helper utilities."""

from __future__ import annotations

import numpy as np
import pytest

from gentropy.method.ldsc_h2 import (
    IRWLS,
    Hsq,
    Jackknife,
    LstsqJackknifeFast,
    _check_shape,
    _check_shape_block,
    run_ldsc_h2_from_arrays,
)


class TestShapeHelpers:
    """Tests for shape validation helper functions."""

    def test_check_shape_valid(self) -> None:
        """Check that _check_shape returns correct n and p for valid input."""
        x = np.zeros((10, 2))
        y = np.zeros((10, 1))
        n, p = _check_shape(x, y)
        assert (n, p) == (10, 2)

    def test_check_shape_1d_raises(self) -> None:
        """Check that _check_shape raises for 1D arrays."""
        x = np.zeros(10)
        y = np.zeros((10, 1))
        # Passing a 1D array should raise at runtime
        with pytest.raises(ValueError, match="x and y must be 2D arrays"):
            _check_shape(x, y)

    def test_check_shape_mismatched_rows_raises(self) -> None:
        """Check that _check_shape raises when x and y have different row counts."""
        x = np.zeros((10, 2))
        y = np.zeros((9, 1))
        with pytest.raises(ValueError, match="Number of datapoints"):
            _check_shape(x, y)

    def test_check_shape_block_valid(self) -> None:
        """Check that _check_shape_block returns expected n_blocks and p."""
        xty = np.zeros((5, 3))
        xtx = np.zeros((5, 3, 3))
        n_blocks, p = _check_shape_block(xty, xtx)
        assert (n_blocks, p) == (5, 3)

    def test_check_shape_block_mismatch_raises(self) -> None:
        """Check that _check_shape_block raises on mismatched shapes."""
        xty = np.zeros((5, 3))
        xtx = np.zeros((4, 3, 3))
        with pytest.raises(ValueError, match="Shape of xty_block_values"):
            _check_shape_block(xty, xtx)

    def test_check_shape_block_not_square_raises(self) -> None:
        """Check that _check_shape_block raises when X'X is not square."""
        xty = np.zeros((5, 3))
        xtx = np.zeros((5, 3, 2))
        with pytest.raises(ValueError, match="Last two axes"):
            _check_shape_block(xty, xtx)


class TestJackknifeCore:
    """Tests for core jackknife helper methods."""

    def test_delete_values_to_pseudovalues_and_jknife(self) -> None:
        """Check pseudovalue construction and jackknife estimate shapes."""
        delete_values = np.array([[1.0], [2.0], [3.0]])
        est = np.array([[2.0]])
        pseudo = Jackknife.delete_values_to_pseudovalues(delete_values, est)
        j_est, j_var, j_se, j_cov = Jackknife.jknife(pseudo)

        assert pseudo.shape == (3, 1)
        assert j_est.shape == (1, 1)
        assert j_var.shape == (1, 1)
        assert j_cov.shape == (1, 1)
        assert j_se.shape == (1, 1)
        # For this simple construction the mean of pseudovalues should equal est
        assert np.allclose(j_est, est)


class TestLstsqJackknifeFast:
    """Tests for the fast block jackknife linear regression implementation."""

    def test_lstsq_jackknife_matches_lstsq_solution(self) -> None:
        """Check that jackknife point estimate matches np.linalg.lstsq."""
        rng = np.random.default_rng(0)
        n = 200
        p = 3
        X = rng.normal(size=(n, p))
        beta_true = np.array([[0.5], [-1.0], [2.0]])
        y = X @ beta_true + rng.normal(scale=0.1, size=(n, 1))

        beta_ols, *_ = np.linalg.lstsq(X, y, rcond=-1)

        jk = LstsqJackknifeFast(X, y, n_blocks=10)
        assert jk.est.shape == (1, p)
        assert np.allclose(jk.est.reshape(-1, 1), beta_ols, rtol=1e-3, atol=1e-3)

        eigvals = np.linalg.eigvalsh(jk.jknife_cov)
        assert np.all(eigvals >= -1e-10)


class TestIRWLS:
    """Tests for the IRWLS class."""

    def test_irwls_reduces_to_wls_when_weights_constant(self) -> None:
        """Check that IRWLS matches WLS when update_func returns constant weights."""
        rng = np.random.default_rng(1)
        n = 300
        p = 2
        X = rng.normal(size=(n, p))
        beta_true = np.array([[1.0], [2.0]])
        y = X @ beta_true + rng.normal(scale=0.2, size=(n, 1))

        def update_func(coef_tuple: tuple[np.ndarray, ...]) -> np.ndarray:
            """Return constant weights to keep IRWLS equivalent to WLS."""
            beta_hat = coef_tuple[0]
            assert beta_hat.shape == (p, 1)
            return np.ones((n, 1))

        w0 = rng.uniform(0.5, 1.5, size=(n, 1))

        irwls = IRWLS(X, y, update_func, n_blocks=10, w=w0)

        beta_wls, *_ = np.linalg.lstsq(X, y, rcond=-1)

        assert irwls.est.shape == (1, p)
        assert np.allclose(irwls.est.reshape(-1, 1), beta_wls, rtol=1e-3, atol=1e-3)


@pytest.fixture
def synthetic_ldsc_data() -> dict[str, np.ndarray]:
    """Generate synthetic LDSC style data that follows the LDSC mean model exactly."""
    rng = np.random.default_rng(42)
    n = 2000

    h2_true = 0.4
    intercept_true = 1.05

    ld = rng.uniform(low=1.0, high=20.0, size=n)
    N = np.full(n, 10_000.0)
    M = 1_000_000.0

    chisq = intercept_true + (N / M) * ld * h2_true

    se = 1.0 / np.sqrt(N)
    z = np.sqrt(chisq)
    signs = rng.choice([-1.0, 1.0], size=n)
    z = z * signs
    beta = z * se

    w_ld = ld.copy()

    return {
        "beta": beta,
        "se": se,
        "N": N,
        "ld": ld,
        "w_ld": w_ld,
        "M": np.array(M, dtype=float),
        "h2_true": np.array(h2_true, dtype=float),
        "intercept_true": np.array(intercept_true, dtype=float),
    }


class TestHsqAndWrapper:
    """Tests for Hsq regression and the run_ldsc_h2_from_arrays wrapper."""

    def test_run_ldsc_h2_from_arrays_recovers_parameters(
        self,
        synthetic_ldsc_data: dict[str, np.ndarray],
    ) -> None:
        """Check that wrapper recovers h2 and intercept on synthetic data."""
        d = synthetic_ldsc_data

        out = run_ldsc_h2_from_arrays(
            beta=d["beta"],
            se=d["se"],
            N=d["N"],
            ld=d["ld"],
            w_ld=d["w_ld"],
            M_ldsc_scalar=float(d["M"]),
            intercept=None,
            twostep=30.0,
            n_blocks=50,
        )

        assert np.isfinite(out["h2"])
        assert np.isfinite(out["intercept"])

        assert np.allclose(out["h2"], d["h2_true"], rtol=5e-2, atol=5e-2)
        assert np.allclose(out["intercept"], d["intercept_true"], rtol=5e-2, atol=5e-2)

        Nbar = float(np.mean(d["N"]))
        slope_expected = Nbar * d["h2_true"] / d["M"]
        assert np.allclose(out["slope"], slope_expected, rtol=5e-2, atol=5e-2)

        chisq_emp = np.mean((d["beta"] / d["se"]) ** 2)
        assert np.allclose(out["mean_chisq"], chisq_emp, rtol=1e-6, atol=1e-6)

    def test_hsq_constrained_intercept_sets_na_se(
        self,
        synthetic_ldsc_data: dict[str, np.ndarray],
    ) -> None:
        """Check that constrained intercept path sets intercept_se and ratio as expected."""
        d = synthetic_ldsc_data
        n = d["beta"].shape[0]
        beta = d["beta"]
        se = d["se"]
        N = d["N"]
        ld = d["ld"]
        w_ld = d["w_ld"]
        M = d["M"]

        z = beta / se
        chisq = z**2

        y = chisq.reshape((n, 1))
        x = ld.reshape((n, 1))
        w = w_ld.reshape((n, 1))
        N_mat = N.reshape((n, 1))
        M_mat = np.array([[float(M)]])

        hsq = Hsq(
            y=y,
            x=x,
            w=w,
            N=N_mat,
            M=M_mat,
            n_blocks=50,
            intercept=float(d["intercept_true"]),
            slow=False,
            twostep=None,
            old_weights=False,
        )

        assert hsq.constrain_intercept is True
        assert hsq.intercept == pytest.approx(float(d["intercept_true"]))
        assert hsq.intercept_se == "NA"
        assert hsq.ratio == "NA"
        assert hsq.ratio_se == "NA"

    def test_hsq_weights_are_positive_and_finite(self) -> None:
        """Check that Hsq.weights returns positive finite weights with expected trend."""
        rng = np.random.default_rng(123)
        n = 500
        ld = rng.uniform(0.0, 50.0, size=(n, 1))
        w_ld = rng.uniform(0.0, 50.0, size=(n, 1))
        N = np.full((n, 1), 20_000.0)
        M = 1_000_000.0
        h2 = 0.5
        intercept = 1.1

        w = Hsq.weights(ld=ld, w_ld=w_ld, N=N, M=M, hsq=h2, intercept=intercept)

        assert w.shape == (n, 1)
        assert np.all(np.isfinite(w))
        assert np.all(w > 0.0)

        order = np.argsort(ld.ravel())
        w_sorted = w.ravel()[order]
        q = n // 4
        mean_low_ld = float(np.mean(w_sorted[:q]))
        mean_high_ld = float(np.mean(w_sorted[-q:]))
        assert mean_high_ld < mean_low_ld


class TestErrorConditions:
    """Tests for various error conditions and input validation."""

    def test_irwls_rejects_non_positive_weights(self) -> None:
        """Check that IRWLS raises if any weights are non positive."""
        X = np.eye(5)
        y = np.ones((5, 1))

        def update_func(result: tuple[np.ndarray, ...]) -> np.ndarray:
            """Return constant positive weights for test helper."""
            return np.ones((5, 1))

        w = np.array([[1.0], [1.0], [0.0], [1.0], [1.0]])

        with pytest.raises(ValueError, match="Weights must be > 0"):
            IRWLS(X, y, update_func, n_blocks=2, w=w)

    def test_run_ldsc_raises_on_non_1d_inputs(
        self,
        synthetic_ldsc_data: dict[str, np.ndarray],
    ) -> None:
        """Check that the convenience wrapper rejects non 1D inputs."""
        d = synthetic_ldsc_data
        beta_2d = d["beta"].reshape(1, -1)

        with pytest.raises(ValueError, match="must be 1D arrays"):
            run_ldsc_h2_from_arrays(
                beta=beta_2d,
                se=d["se"],
                N=d["N"],
                ld=d["ld"],
                w_ld=d["w_ld"],
                M_ldsc_scalar=float(d["M"]),
            )
