"""Common fixtures for Finngen tests."""

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def urlopen_mock(
    efo_mappings_mock: list[tuple[str, str, str, str]],
    finngen_phenotype_table_mock: str,
) -> Callable[[str], MagicMock]:
    """Mock object for requesting urlopen objects with proper encoding.

    This mock object allows to call `read` and `readlines` methods on two endpoints:
    - https://finngen_phenotypes -> finngen_phenotype_table_mock
    - https://efo_mappings -> efo_mappings_mock

    The return values are mocks of the source data respectively.
    """

    def mock_response(url: str) -> MagicMock:
        """Mock urllib.request.urlopen."""
        match url:
            case "https://finngen_phenotypes":
                value = finngen_phenotype_table_mock
            case "https://efo_mappings":
                value = "\n".join(["\t".join(row) for row in efo_mappings_mock])
            case _:
                value = ""
        mock_open = MagicMock()
        mock_open.read.return_value = value.encode()
        mock_open.readlines.return_value = value.encode().splitlines(keepends=True)
        return mock_open

    return mock_response
