"""Tests for the LD index."""

from __future__ import annotations

import pytest

from gentropy.dataset.ld_index import LDIndex


@pytest.mark.legacy
def test_ld_index_creation(mock_ld_index: LDIndex) -> None:
    """Randomly generated panel data fits the LD index schema."""
    assert isinstance(mock_ld_index, LDIndex)
    assert mock_ld_index.df.count() == 400
