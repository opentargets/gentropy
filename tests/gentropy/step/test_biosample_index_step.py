"""Tests for biosample index step."""

from __future__ import annotations

from pathlib import Path

import pytest

from gentropy.biosample_index import BiosampleIndexDefaults, BiosampleIndexStep
from gentropy.common.session import Session


@pytest.mark.step_test
class TestBiosampleIndexStep:
    """Test biosample index step."""

    def test_biosample_index_defaults_validation(self) -> None:
        """Test that BiosampleIndexDefaults validates required fields."""
        with pytest.raises(Exception):
            BiosampleIndexDefaults()

    def test_biosample_index_defaults_with_required(self, tmp_path: Path) -> None:
        """Test that BiosampleIndexDefaults can be created with all required fields."""
        config = BiosampleIndexDefaults(
            cell_ontology_input_path=str(tmp_path / "cell_ontology.json"),
            uberon_input_path=str(tmp_path / "uberon.json"),
            efo_input_path=str(tmp_path / "efo.json"),
            biosample_index_path=str(tmp_path / "biosample_index"),
        )
        assert config.cell_ontology_input_path == str(tmp_path / "cell_ontology.json")

    def test_biosample_index_step_initialization(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that BiosampleIndexStep can be initialized."""
        config = BiosampleIndexDefaults(
            cell_ontology_input_path=str(tmp_path / "cell_ontology.json"),
            uberon_input_path=str(tmp_path / "uberon.json"),
            efo_input_path=str(tmp_path / "efo.json"),
            biosample_index_path=str(tmp_path / "biosample_index"),
        )
        # This test verifies that the step raises an exception when files don't exist
        with pytest.raises(Exception):
            BiosampleIndexStep(config=config, session=session)
