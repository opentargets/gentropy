"""Tests for interval steps."""

from __future__ import annotations

from pathlib import Path

import pytest

from gentropy.common.session import Session
from gentropy.intervals import IntervalE2GStep


@pytest.mark.step_test
class TestIntervalE2GStep:
    """Test interval E2G step."""

    def test_interval_e2g_step_initialization(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that IntervalE2GStep raises an exception when files don't exist."""
        # Create temporary paths
        target_index_path = str(tmp_path / "target_index")
        biosample_mapping_path = str(tmp_path / "biosample_mapping.csv")
        biosample_index_path = str(tmp_path / "biosample_index")
        chromosome_contig_index_path = str(tmp_path / "chromosome_contig_index")
        interval_source = str(tmp_path / "interval_source")
        valid_output_path = str(tmp_path / "valid_output")
        invalid_output_path = str(tmp_path / "invalid_output")

        # This test verifies that the step raises an exception when files don't exist
        with pytest.raises(Exception):
            # Expected when data files don't exist - this is normal behavior
            IntervalE2GStep(
                session=session,
                target_index_path=target_index_path,
                biosample_mapping_path=biosample_mapping_path,
                biosample_index_path=biosample_index_path,
                chromosome_contig_index_path=chromosome_contig_index_path,
                interval_source=interval_source,
                valid_output_path=valid_output_path,
                invalid_output_path=invalid_output_path,
            )

    def test_interval_e2g_step_parameters(self) -> None:
        """Test that IntervalE2GStep has correct expected parameters."""
        import inspect

        from gentropy.intervals import IntervalE2GStep

        sig = inspect.signature(IntervalE2GStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "target_index_path",
            "biosample_mapping_path",
            "biosample_index_path",
            "chromosome_contig_index_path",
            "interval_source",
            "valid_output_path",
            "invalid_output_path",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"
