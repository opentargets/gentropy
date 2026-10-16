"""Tests for variant_index step."""

from __future__ import annotations

import pytest

from gentropy.variant_index import ConvertToVcfStep


@pytest.mark.step_test
class TestConvertToVcfStep:
    """Test ConvertToVcfStep initialization and parameter validation."""

    def test_convert_to_vcf_step_parameters(self) -> None:
        """Test that ConvertToVcfStep has correct expected parameters."""
        import inspect

        sig = inspect.signature(ConvertToVcfStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "source_paths",
            "source_formats",
            "output_path",
            "partition_size",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"
