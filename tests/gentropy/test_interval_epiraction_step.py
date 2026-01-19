"""Extended tests for interval steps."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy.common.session import Session
from gentropy.intervals import IntervalEpiractionStep


@pytest.mark.step_test
class TestIntervalEpiractionStep:
    """Test IntervalEpiractionStep initialization and parameter validation."""

    def test_interval_epiraction_step_initialization(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that IntervalEpiractionStep can be initialized without errors."""
        target_index_path = str(tmp_path / "target_index")
        interval_source = str(tmp_path / "interval_source")
        interval_epiraction_path = str(tmp_path / "interval_epiraction")

        with (
            patch("gentropy.intervals.TargetIndex.from_parquet") as mock_target_index,
            patch("gentropy.intervals.IntervalsEpiraction.read") as mock_read,
            patch("gentropy.intervals.IntervalsEpiraction.parse") as mock_parse,
        ):
            mock_target = MagicMock()
            mock_target.persist.return_value = mock_target
            mock_target_index.return_value = mock_target

            mock_data = MagicMock()
            mock_read.return_value = mock_data

            mock_epiraction = MagicMock()
            mock_epiraction.df = MagicMock()
            mock_parse.return_value = mock_epiraction

            step = IntervalEpiractionStep(
                session=session,
                target_index_path=target_index_path,
                interval_source=interval_source,
                interval_epiraction_path=interval_epiraction_path,
            )
            assert step is not None

    def test_interval_epiraction_step_parameters(self) -> None:
        """Test that IntervalEpiractionStep has correct expected parameters."""
        import inspect

        from gentropy.intervals import IntervalEpiractionStep

        sig = inspect.signature(IntervalEpiractionStep.__init__)
        params = list(sig.parameters.keys())

        expected_params = [
            "self",
            "session",
            "target_index_path",
            "interval_source",
            "interval_epiraction_path",
        ]

        for param in expected_params:
            assert param in params, f"Missing parameter: {param}"

    def test_interval_epiraction_step_data_flow(
        self, session: Session, tmp_path: Path
    ) -> None:
        """Test that IntervalEpiractionStep data flows correctly."""
        target_index_path = str(tmp_path / "target_index")
        interval_source = str(tmp_path / "interval_source")
        interval_epiraction_path = str(tmp_path / "interval_epiraction")

        with (
            patch("gentropy.intervals.TargetIndex.from_parquet") as mock_target_index,
            patch("gentropy.intervals.IntervalsEpiraction.read") as mock_read,
            patch("gentropy.intervals.IntervalsEpiraction.parse") as mock_parse,
        ):
            mock_target = MagicMock()
            mock_target.persist.return_value = mock_target
            mock_target_index.return_value = mock_target

            mock_data = MagicMock()
            mock_read.return_value = mock_data

            mock_epiraction = MagicMock()
            mock_epiraction.df = MagicMock()
            mock_parse.return_value = mock_epiraction

            IntervalEpiractionStep(
                session=session,
                target_index_path=target_index_path,
                interval_source=interval_source,
                interval_epiraction_path=interval_epiraction_path,
            )

            # Verify the data flow
            mock_target_index.assert_called_once_with(session, target_index_path)
            mock_target.persist.assert_called_once()
            mock_read.assert_called_once_with(session.spark, interval_source)
            mock_parse.assert_called_once_with(mock_data, mock_target)
            mock_epiraction.df.write.mode.assert_called_once_with(session.write_mode)
