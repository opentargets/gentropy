"""Tests for common stats functions."""

from __future__ import annotations

import numpy as np
import pytest

from gentropy.common.stats import get_logsum, split_pvalue


@pytest.mark.usefixtures("session")
class TestStats:
    """Test common statistics functions."""

    def test_get_logsum_single_value(self) -> None:
        """Test logsumexp calculation with single value."""
        arr = np.array([1.0])
        result = get_logsum(arr)
        assert pytest.approx(result, rel=1e-6) == 1.0

    def test_get_logsum_multiple_values(self) -> None:
        """Test logsumexp calculation with multiple values."""
        arr = np.array([0.2, 0.1, 0.05, 0.0])
        result = get_logsum(arr)
        # Expected value from docstring
        assert pytest.approx(result, rel=1e-6) == 1.476557

    def test_get_logsum_large_values(self) -> None:
        """Test logsumexp calculation with large values (prevent overflow)."""
        arr = np.array([1000.0, 1000.1, 999.9])
        result = get_logsum(arr)
        # Should not be infinity, indicating proper handling of overflow
        assert np.isfinite(result)
        assert result > 1000.0  # Should be close to max value plus log of sum

    def test_get_logsum_negative_values(self) -> None:
        """Test logsumexp calculation with negative values."""
        arr = np.array([-1.0, -2.0, -3.0])
        result = get_logsum(arr)
        assert np.isfinite(result)

    def test_split_pvalue_small_value(self) -> None:
        """Test split_pvalue with small p-value."""
        mantissa, exponent = split_pvalue(0.00001234)
        assert mantissa == 1.234
        assert exponent == -5

    def test_split_pvalue_unit(self) -> None:
        """Test split_pvalue with p-value = 1."""
        mantissa, exponent = split_pvalue(1.0)
        assert mantissa == 1.0
        assert exponent == 0

    def test_split_pvalue_decimal(self) -> None:
        """Test split_pvalue with decimal p-value."""
        mantissa, exponent = split_pvalue(0.123)
        assert mantissa == 1.23
        assert exponent == -1

    def test_split_pvalue_near_unit(self) -> None:
        """Test split_pvalue with p-value close to 1."""
        mantissa, exponent = split_pvalue(0.99)
        assert mantissa == 9.9
        assert exponent == -1

    def test_split_pvalue_very_small(self) -> None:
        """Test split_pvalue with very small p-value."""
        mantissa, exponent = split_pvalue(1e-300)
        assert mantissa == 1.0
        assert exponent == -300

    def test_split_pvalue_invalid_below_zero(self) -> None:
        """Test split_pvalue with p-value < 0."""
        with pytest.raises(ValueError, match="P-value must be between 0 and 1"):
            split_pvalue(-0.01)

    def test_split_pvalue_invalid_above_one(self) -> None:
        """Test split_pvalue with p-value > 1."""
        with pytest.raises(ValueError, match="P-value must be between 0 and 1"):
            split_pvalue(1.1)

    def test_split_pvalue_zero(self) -> None:
        """Test split_pvalue with p-value = 0."""
        mantissa, exponent = split_pvalue(0.0)
        assert mantissa == 0.0
        assert exponent == 0

    def test_split_pvalue_mantissa_range(self) -> None:
        """Test that split_pvalue always returns mantissa between 1 and 10."""
        test_pvalues = [0.001, 0.01, 0.05, 0.1, 0.5, 0.99, 1e-10, 1e-100]
        for pval in test_pvalues:
            mantissa, exponent = split_pvalue(pval)
            if mantissa != 0.0:  # Skip zero case
                assert 1.0 <= mantissa <= 10.0, (
                    f"Mantissa {mantissa} out of range for p-value {pval}"
                )
