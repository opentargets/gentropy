"""Tests for statistics helpers."""

from __future__ import annotations

import pytest

from gentropy.common.stats import split_pvalue

# Tests ported from the legacy package (legacy_tests/gentropy/common/test_stats.py).


@pytest.mark.legacy
def test_split_pvalue_very_small_exponent() -> None:
    """P-values near the floating-point limit keep their exponent."""
    assert split_pvalue(1e-300) == (1.0, -300)


@pytest.mark.legacy
@pytest.mark.parametrize("pvalue", [-0.01, 1.1], ids=["below zero", "above one"])
def test_split_pvalue_rejects_values_outside_zero_and_one(pvalue: float) -> None:
    """Only probabilities can be split."""
    with pytest.raises(ValueError, match="P-value must be between 0 and 1"):
        split_pvalue(pvalue)
