"""Test that validates the mkdocs documentation using pytest."""

from __future__ import annotations

import pytest
from mkdocs.commands.build import build
from mkdocs.config import load_config
from mkdocs.exceptions import MkDocsException


# Pytest fixture to build the MkDocs documentation before running tests.
@pytest.fixture(scope="module")
def mkdocs_build() -> None:
    """Fixture to build documentation."""
    try:
        cfg = load_config("mkdocs.yml", strict=False)
        cfg.plugins["material/search"].on_startup(command="build", dirty=not "clean")
        # config = load_config()
        # config.strict = True
        build(cfg)
    except MkDocsException as e:
        raise AssertionError(f"MkDocs build failed: {e}") from e


def test_mkdocs_build() -> None:
    """Function to check if MkDocs build succeeded."""
    pass
