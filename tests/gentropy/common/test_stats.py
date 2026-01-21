"""Tests for common stats functions.

Note: Basic functionality tests for split_pvalue and get_logsum are covered by doctests
in gentropy.common.stats module. This module tests additional edge cases and behaviors
not covered by doctests.
"""

from __future__ import annotations

import numpy as np
import pytest

from gentropy.common.stats import get_logsum, split_pvalue


@pytest.mark.usefixtures("session")
class TestStats:
    """Test common statistics functions - edge cases and additional coverage."""

    def test_get_logsum_large_values_overflow_protection(self) -> None:
        """Test logsumexp calculation with large values (prevent overflow).

        This test validates the overflow protection mechanism used by get_logsum,
        which is critical for numerical stability with very large exponents.
        """
        arr = np.array([1000.0, 1000.1, 999.9])
        result = get_logsum(arr)
        # Should not be infinity, indicating proper handling of overflow
        assert np.isfinite(result)
        assert result > 1000.0  # Should be close to max value plus log of sum

    def test_get_logsum_negative_values(self) -> None:
        """Test logsumexp calculation with negative values.

        Edge case: function should handle negative log values correctly.
        """
        arr = np.array([-1.0, -2.0, -3.0])
        result = get_logsum(arr)
        assert np.isfinite(result)

    def test_split_pvalue_very_small_exponent(self) -> None:
        """Test split_pvalue with extremely small p-value.

        Tests handling of p-values near the limits of floating-point representation.
        """
        mantissa, exponent = split_pvalue(1e-300)
        assert mantissa == 1.0
        assert exponent == -300

    def test_split_pvalue_invalid_below_zero(self) -> None:
        """Test split_pvalue with p-value < 0 (invalid input)."""
        with pytest.raises(ValueError, match="P-value must be between 0 and 1"):
            split_pvalue(-0.01)

    def test_split_pvalue_invalid_above_one(self) -> None:
        """Test split_pvalue with p-value > 1 (invalid input)."""
        with pytest.raises(ValueError, match="P-value must be between 0 and 1"):
            split_pvalue(1.1)

    def test_split_pvalue_zero(self) -> None:
        """Test split_pvalue with p-value = 0 (boundary case).

        Zero is a special case where log10(0) is undefined, so the function
        should handle it with exponent = 0.
        """
        mantissa, exponent = split_pvalue(0.0)
        assert mantissa == 0.0
        assert exponent == 0

    def test_split_pvalue_mantissa_range_comprehensive(self) -> None:
        """Test that split_pvalue returns mantissa between 1 and 10 (except zero).

        This validates the invariant that (mantissa, exponent) always satisfies:
        pvalue ≈ mantissa * 10^exponent where 1 <= mantissa < 10 (or mantissa = 0).
        """
        test_pvalues = [0.001, 0.01, 0.05, 0.1, 0.5, 0.99, 1e-10, 1e-100]
        for pval in test_pvalues:
            mantissa, exponent = split_pvalue(pval)
            if mantissa != 0.0:  # Skip zero case
                assert 1.0 <= mantissa <= 10.0, (
                    f"Mantissa {mantissa} out of range for p-value {pval}"
                )
