"""Tests on Biosample index."""

from gentropy.dataset.biosample_index import BiosampleIndex


def test_biosample_index_creation(mock_biosample_index: BiosampleIndex) -> None:
    """Test biosample index creation with mock biosample index."""
    assert isinstance(mock_biosample_index, BiosampleIndex)
