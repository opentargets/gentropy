"""Tests for SessionDefaults configuration model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from gentropy.config import SessionDefaults


class TestSessionDefaults:
    """Test SessionDefaults Pydantic model."""

    def test_session_defaults_default_values(self) -> None:
        """Test creating a SessionDefaults with all defaults."""
        defaults = SessionDefaults()

        assert defaults.spark_uri == "local[*]"
        assert defaults.write_mode == "errorifexists"
        assert defaults.output_partitions == 200
        assert defaults.start_hail is False
        assert defaults.dynamic_allocation is True
        assert defaults.log_level == "ERROR"

    def test_session_defaults_custom_values(self) -> None:
        """Test creating SessionDefaults with custom values."""
        defaults = SessionDefaults(
            spark_uri="local[4]",
            write_mode="overwrite",
            output_partitions=100,
            start_hail=True,
            dynamic_allocation=False,
            log_level="DEBUG",
        )

        assert defaults.spark_uri == "local[4]"
        assert defaults.write_mode == "overwrite"
        assert defaults.output_partitions == 100
        assert defaults.start_hail is True
        assert defaults.dynamic_allocation is False
        assert defaults.log_level == "DEBUG"

    def test_session_defaults_frozen(self) -> None:
        """Test that SessionDefaults is immutable."""
        defaults = SessionDefaults()
        with pytest.raises(ValidationError):
            defaults.spark_uri = "local[*]"  # type: ignore[misc]

    def test_session_defaults_extended_spark_conf_default(self) -> None:
        """Test that extended_spark_conf defaults to empty dict."""
        defaults = SessionDefaults()
        assert defaults.extended_spark_conf == {}

    def test_session_defaults_extended_hail_conf_default(self) -> None:
        """Test that extended_hail_conf defaults to empty dict."""
        defaults = SessionDefaults()
        assert defaults.extended_hail_conf == {}

    def test_session_defaults_output_partitions_minimum(self) -> None:
        """Test that output_partitions must be >= 1."""
        with pytest.raises(ValidationError):
            SessionDefaults(output_partitions=0)

    def test_session_defaults_hail_home(self) -> None:
        """Test that hail_home defaults to Hail installation directory."""
        defaults = SessionDefaults()
        assert defaults.hail_home is not None
