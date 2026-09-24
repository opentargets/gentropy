"""Tests for LDSC genetic correlation estimation."""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

from gentropy.method.ldsc import run_ldsc_rg_from_arrays
from gentropy.method.ldsc.regression import GeneticCov


@pytest.fixture
def synthetic_rg_data() -> dict[str, Any]:
    """Generate two correlated traits with known rg ~ 0.5."""
    rng = np.random.default_rng(99)
    n = 3000
    M = 1_000_000.0
    h2_1 = 0.4
    h2_2 = 0.3
    rg_true = 0.5

    ld = rng.uniform(1.0, 20.0, size=n)
    N1 = np.full(n, 50_000.0)
    N2 = np.full(n, 40_000.0)

    # Genetic component
    g1 = rng.normal(scale=np.sqrt(h2_1 * ld / M), size=n)
    g2 = rg_true * g1 + rng.normal(
        scale=np.sqrt(h2_2 * ld / M * (1 - rg_true**2)), size=n
    )

    # Z scores: genetic + noise
    z1 = np.sqrt(N1) * g1 + rng.normal(size=n)
    z2 = np.sqrt(N2) * g2 + rng.normal(size=n)

    w_ld = ld.copy()

    return {
        "z1": z1,
        "N1": N1,
        "z2": z2,
        "N2": N2,
        "ld": ld,
        "w_ld": w_ld,
        "M": M,
        "rg_true": rg_true,
        "h2_1_true": h2_1,
        "h2_2_true": h2_2,
    }


class TestRunLdscRg:
    """Tests for the run_ldsc_rg_from_arrays convenience wrapper."""

    def test_returns_required_keys(self, synthetic_rg_data: dict[str, Any]) -> None:
        """Check that the wrapper returns all expected output keys."""
        d = synthetic_rg_data
        out = run_ldsc_rg_from_arrays(
            z1=d["z1"],
            N1=d["N1"],
            z2=d["z2"],
            N2=d["N2"],
            ld=d["ld"],
            w_ld=d["w_ld"],
            M_ldsc_scalar=d["M"],
            n_blocks=50,
        )
        for key in (
            "rg",
            "rg_se",
            "gcov",
            "gcov_se",
            "h2_1",
            "h2_2",
            "intercept",
            "n_snps",
        ):
            assert key in out

    def test_rg_is_finite(self, synthetic_rg_data: dict[str, Any]) -> None:
        """Check that rg and rg_se are finite and rg is bounded to [-1, 1]."""
        d = synthetic_rg_data
        out = run_ldsc_rg_from_arrays(
            z1=d["z1"],
            N1=d["N1"],
            z2=d["z2"],
            N2=d["N2"],
            ld=d["ld"],
            w_ld=d["w_ld"],
            M_ldsc_scalar=d["M"],
            n_blocks=50,
        )
        assert np.isfinite(out["rg"])
        assert np.isfinite(out["rg_se"])
        assert -1.0 <= out["rg"] <= 1.0

    def test_rg_positive_for_correlated_traits(
        self, synthetic_rg_data: dict[str, Any]
    ) -> None:
        """Check that rg is positive when traits are positively correlated."""
        d = synthetic_rg_data
        out = run_ldsc_rg_from_arrays(
            z1=d["z1"],
            N1=d["N1"],
            z2=d["z2"],
            N2=d["N2"],
            ld=d["ld"],
            w_ld=d["w_ld"],
            M_ldsc_scalar=d["M"],
            n_blocks=50,
        )
        assert out["rg"] > 0

    def test_raises_on_non_1d_input(self, synthetic_rg_data: dict[str, Any]) -> None:
        """Check that the wrapper raises ValueError for non-1D input arrays."""
        d = synthetic_rg_data
        with pytest.raises(ValueError, match="1D"):
            run_ldsc_rg_from_arrays(
                z1=d["z1"].reshape(1, -1),
                N1=d["N1"],
                z2=d["z2"],
                N2=d["N2"],
                ld=d["ld"],
                w_ld=d["w_ld"],
                M_ldsc_scalar=d["M"],
            )


class TestGeneticCovWeights:
    """Tests for the GeneticCov.weights class method."""

    def test_weights_are_positive_finite(self) -> None:
        """Check that GeneticCov.weights returns positive finite values."""
        rng = np.random.default_rng(7)
        n = 200
        ld = rng.uniform(1.0, 20.0, size=(n, 1))
        w_ld = rng.uniform(1.0, 20.0, size=(n, 1))
        N1 = np.full((n, 1), 50_000.0)
        N2 = np.full((n, 1), 40_000.0)
        w = GeneticCov.weights(ld, w_ld, N1, N2, 1_000_000.0, 0.4, 0.3, 0.0)
        assert w.shape == (n, 1)
        assert np.all(np.isfinite(w))
        assert np.all(w > 0)
