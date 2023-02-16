"""Test colocalisation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from otg.dataset.colocalisation import Colocalisation


def test_colocalisation_creation(mock_colocalisation: Colocalisation) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_colocalisation, Colocalisation)
