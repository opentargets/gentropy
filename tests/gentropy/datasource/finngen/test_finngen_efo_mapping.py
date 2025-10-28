"""EFO mapping tests."""

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from gentropy import Session
from gentropy.datasource.finngen.efo_mapping import EFOMapping


def test_efo_mapping_from_path(
    monkeypatch: pytest.MonkeyPatch,
    session: Session,
    urlopen_mock: Callable[[str], MagicMock],
) -> None:
    """Test reading efo curation."""
    with monkeypatch.context() as m:
        m.setattr("gentropy.datasource.finngen.efo_mapping.urlopen", urlopen_mock)
        efo_df = EFOMapping.from_path(session, "https://efo_mappings")
        assert isinstance(efo_df, EFOMapping)
        efo_df.df.show()
        assert efo_df.df.count() == 5
