"""Tests for LD-based clumping step."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy.common.session import Session
from gentropy.ld_based_clumping import LDBasedClumpingStep


@pytest.mark.step_test
class TestLDBasedClumpingStep:
    """Test LDBasedClumpingStep initialization and parameter validation."""

    def test_ld_based_clumping_step_initialization(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that LDBasedClumpingStep initializes without errors."""
        study_locus_input_path = str(tmp_path / "study_locus")
        study_index_path = str(tmp_path / "study_index")
        ld_index_path = str(tmp_path / "ld_index")
        clumped_output_path = str(tmp_path / "clumped")

        with (
            patch(
                "gentropy.ld_based_clumping.StudyLocus.from_parquet"
            ) as mock_study_locus,
            patch("gentropy.ld_based_clumping.LDIndex.from_parquet") as mock_ld_index,
            patch(
                "gentropy.ld_based_clumping.StudyIndex.from_parquet"
            ) as mock_study_index,
        ):
            # Mock the dataframe objects
            mock_sl = MagicMock()
            mock_sl.annotate_ld.return_value = mock_sl
            mock_sl.clump.return_value = mock_sl
            mock_sl.df = MagicMock()

            mock_study_locus.return_value = mock_sl
            mock_ld_index.return_value = MagicMock()
            mock_study_index.return_value = MagicMock()

            step = LDBasedClumpingStep(
                session=session,
                study_locus_input_path=study_locus_input_path,
                study_index_path=study_index_path,
                ld_index_path=ld_index_path,
                clumped_study_locus_output_path=clumped_output_path,
            )
            assert step is not None

    def test_ld_based_clumping_step_parameters(self) -> None:
        """Test that LDBasedClumpingStep has correct expected parameters."""
        import inspect

        sig = inspect.signature(LDBasedClumpingStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "study_locus_input_path",
            "study_index_path",
            "ld_index_path",
            "clumped_study_locus_output_path",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"

    def test_ld_based_clumping_step_methods_called(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that the step calls the expected methods in correct sequence."""
        study_locus_input_path = str(tmp_path / "study_locus")
        study_index_path = str(tmp_path / "study_index")
        ld_index_path = str(tmp_path / "ld_index")
        clumped_output_path = str(tmp_path / "clumped")

        with (
            patch(
                "gentropy.ld_based_clumping.StudyLocus.from_parquet"
            ) as mock_study_locus,
            patch("gentropy.ld_based_clumping.LDIndex.from_parquet") as mock_ld_index,
            patch(
                "gentropy.ld_based_clumping.StudyIndex.from_parquet"
            ) as mock_study_index,
        ):
            # Setup mock chain
            mock_sl = MagicMock()
            mock_annotated = MagicMock()
            mock_clumped = MagicMock()
            mock_clumped.df = MagicMock()

            mock_sl.annotate_ld.return_value = mock_annotated
            mock_annotated.clump.return_value = mock_clumped

            mock_study_locus.return_value = mock_sl
            mock_ld_index.return_value = MagicMock()
            mock_study_index.return_value = MagicMock()

            LDBasedClumpingStep(
                session=session,
                study_locus_input_path=study_locus_input_path,
                study_index_path=study_index_path,
                ld_index_path=ld_index_path,
                clumped_study_locus_output_path=clumped_output_path,
            )

            # Verify methods were called in correct order
            mock_sl.annotate_ld.assert_called_once()
            mock_annotated.clump.assert_called_once()
