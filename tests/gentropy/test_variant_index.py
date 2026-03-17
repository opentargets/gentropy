"""Tests for variant_index step."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy.common.session import Session
from gentropy.variant_index import ConvertToVcfStep, VariantIndexStep


@pytest.mark.step_test
class TestVariantIndexStep:
    """Test VariantIndexStep initialization and parameter validation."""

    def test_variant_index_step_initialization(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that VariantIndexStep initializes without errors when files don't exist."""
        vep_output_json_path = str(tmp_path / "vep_output.json")
        variant_index_path = str(tmp_path / "variant_index")

        # Mock the VEP parser to avoid file I/O
        with patch(
            "gentropy.variant_index.VariantEffectPredictorParser.extract_variant_index_from_vep"
        ) as mock_vep:
            mock_vep.return_value = MagicMock()
            mock_vep.return_value.df = MagicMock()

            step = VariantIndexStep(
                session=session,
                vep_output_json_path=vep_output_json_path,
                variant_index_path=variant_index_path,
                hash_threshold=20,
            )
            assert step is not None

    def test_variant_index_step_initialization_with_annotations(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test VariantIndexStep with variant annotations."""
        vep_output_json_path = str(tmp_path / "vep_output.json")
        variant_index_path = str(tmp_path / "variant_index")
        annotation_path = [str(tmp_path / "annotation1")]

        with (
            patch(
                "gentropy.variant_index.VariantEffectPredictorParser.extract_variant_index_from_vep"
            ) as mock_vep,
            patch(
                "gentropy.variant_index.VariantIndex.from_parquet"
            ) as mock_from_parquet,
        ):
            mock_variant_index = MagicMock()
            mock_variant_index.df = MagicMock()
            mock_variant_index.add_annotation.return_value = mock_variant_index
            mock_vep.return_value = mock_variant_index
            mock_from_parquet.return_value = mock_variant_index

            step = VariantIndexStep(
                session=session,
                vep_output_json_path=vep_output_json_path,
                variant_index_path=variant_index_path,
                hash_threshold=20,
                variant_annotations_path=annotation_path,
            )
            assert step is not None
            mock_variant_index.add_annotation.assert_called_once()

    def test_variant_index_step_initialization_with_amino_acids(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test VariantIndexStep with amino acid annotations."""
        vep_output_json_path = str(tmp_path / "vep_output.json")
        variant_index_path = str(tmp_path / "variant_index")
        amino_acid_path = [str(tmp_path / "amino_acid")]

        with (
            patch(
                "gentropy.variant_index.VariantEffectPredictorParser.extract_variant_index_from_vep"
            ) as mock_vep,
            patch(
                "gentropy.variant_index.AminoAcidVariants.from_parquet"
            ) as mock_amino_acids,
        ):
            mock_variant_index = MagicMock()
            mock_variant_index.df = MagicMock()
            mock_variant_index.annotate_with_amino_acid_consequences.return_value = (
                mock_variant_index
            )
            mock_vep.return_value = mock_variant_index
            mock_amino_acids.return_value = MagicMock()

            step = VariantIndexStep(
                session=session,
                vep_output_json_path=vep_output_json_path,
                variant_index_path=variant_index_path,
                hash_threshold=20,
                amino_acid_change_annotations=amino_acid_path,
            )
            assert step is not None
            mock_variant_index.annotate_with_amino_acid_consequences.assert_called_once()

    def test_variant_index_step_parameters(self) -> None:
        """Test that VariantIndexStep has correct expected parameters."""
        import inspect

        sig = inspect.signature(VariantIndexStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "vep_output_json_path",
            "variant_index_path",
            "hash_threshold",
            "variant_annotations_path",
            "amino_acid_change_annotations",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"


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
