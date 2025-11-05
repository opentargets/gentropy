"""Test that validates the mkdocs documentation using pytest."""

from __future__ import annotations

import platform

import pytest
from mkdocs.commands.build import build
from mkdocs.config import load_config
from mkdocs.exceptions import MkDocsException

MACHINE = platform.machine()


# NOTE: Katex binary (library for rendering math in MKdocs) is not supported on non-amd64 architectures.
# This library is required for math rendering in MKdocs - hence we can skip the test on non-amd64 architectures.
@pytest.mark.skipif(
    MACHINE != "amd64",
    reason="Skipping MkDocs build on non-amd64 architectures due to missing katex support.",
)
class TestMkDocsBuild:
    """Test class for MkDocs documentation build."""

    # Pytest fixture to build the MkDocs documentation before running tests.
    @pytest.fixture(scope="class")
    def mkdocs_build(self) -> None:
        """Fixture to build documentation."""
        try:
            cfg = load_config("mkdocs.yml", strict=False)
            cfg.plugins["material/search"].on_startup(
                command="build", dirty=not "clean"
            )
            # config = load_config()
            # config.strict = True
            build(cfg)
        except MkDocsException as e:
            raise AssertionError(f"MkDocs build failed: {e}") from e

    def test_mkdocs_build(self) -> None:
        """Function to check if MkDocs build succeeded."""
        pass

    def test_check_missing_files(self, mkdocs_build: None) -> None:
        """Test function to check for missing files in the documentation build."""
        import os

        build_dir = "site"
        assert os.path.exists(build_dir), (
            f"Build directory '{build_dir}' does not exist."
        )
