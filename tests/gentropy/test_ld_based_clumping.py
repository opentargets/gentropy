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
                "gentropy.ld_based_clumping.AnnotatedStudyLocus.from_parquet"
            ) as mock_study_locus,
            patch("gentropy.ld_based_clumping.LDIndex.from_parquet") as mock_ld_index,
            patch(
                "gentropy.ld_based_clumping.StudyIndex.from_parquet"
            ) as mock_study_index,
            patch("gentropy.ld_based_clumping.annotate_ld"),
            patch("gentropy.ld_based_clumping.LDClumping"),
        ):
            mock_study_locus.return_value = MagicMock()
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
                "gentropy.ld_based_clumping.AnnotatedStudyLocus.from_parquet"
            ) as mock_study_locus,
            patch("gentropy.ld_based_clumping.LDIndex.from_parquet") as mock_ld_index,
            patch(
                "gentropy.ld_based_clumping.StudyIndex.from_parquet"
            ) as mock_study_index,
            patch("gentropy.ld_based_clumping.annotate_ld") as mock_annotate_ld,
            patch("gentropy.ld_based_clumping.LDClumping") as mock_ld_clumping,
        ):
            mock_sl = MagicMock()
            mock_si = MagicMock()
            mock_ld = MagicMock()
            mock_study_locus.return_value = mock_sl
            mock_study_index.return_value = mock_si
            mock_ld_index.return_value = mock_ld

            LDBasedClumpingStep(
                session=session,
                study_locus_input_path=study_locus_input_path,
                study_index_path=study_index_path,
                ld_index_path=ld_index_path,
                clumped_study_locus_output_path=clumped_output_path,
            )

            # LD annotation feeds LD clumping, which keeps and flags clumped loci:
            mock_annotate_ld.assert_called_once_with(mock_sl, mock_si, mock_ld)
            mock_ld_clumping.assert_called_once_with(keep_clumped_loci=True)
            mock_ld_clumping.return_value.clump.assert_called_once_with(
                mock_annotate_ld.return_value
            )
