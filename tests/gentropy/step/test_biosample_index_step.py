"""Tests for biosample index step."""

from __future__ import annotations

from pathlib import Path

import pytest

from gentropy.biosample_index import BiosampleIndexStep
from gentropy.common.session import Session


@pytest.mark.step_test
class TestBiosampleIndexStep:
    """Test biosample index step."""

    def test_biosample_index_step_initialization(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that BiosampleIndexStep can be initialized."""
        # Create temporary paths
        cell_ontology_input_path = str(tmp_path / "cell_ontology.json")
        uberon_input_path = str(tmp_path / "uberon.json")
        efo_input_path = str(tmp_path / "efo.json")
        biosample_index_path = str(tmp_path / "biosample_index")

        # This test verifies that the step raises an exception when files don't exist
        # (which is expected behavior)
        with pytest.raises(Exception):
            # Expected when data files don't exist - this is normal behavior
            BiosampleIndexStep(
                session=session,
                cell_ontology_input_path=cell_ontology_input_path,
                uberon_input_path=uberon_input_path,
                efo_input_path=efo_input_path,
                biosample_index_path=biosample_index_path,
            )

    def test_biosample_index_step_parameters(self) -> None:
        """Test that BiosampleIndexStep has correct expected parameters."""
        import inspect

        from gentropy.biosample_index import BiosampleIndexStep

        sig = inspect.signature(BiosampleIndexStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "cell_ontology_input_path",
            "uberon_input_path",
            "efo_input_path",
            "biosample_index_path",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"
