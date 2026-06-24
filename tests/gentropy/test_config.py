"""Tests for configuration module."""

from __future__ import annotations

import importlib
import sys
from dataclasses import fields, is_dataclass

import pytest

from gentropy.config import (
    BiosampleIndexConfig,
    ColocalisationConfig,
    Config,
    SessionConfig,
    StepConfig,
    register_config,
)


class TestSessionConfig:
    """Test SessionConfig dataclass."""

    def test_session_config_creation(self) -> None:
        """Test creating a SessionConfig."""
        config = SessionConfig()

        assert config.start_hail is False
        assert config.write_mode == "errorifexists"
        assert config.spark_uri == "local[*]"
        assert config.output_partitions == 200

    def test_session_config_is_dataclass(self) -> None:
        """Test that SessionConfig is a dataclass."""
        assert is_dataclass(SessionConfig)

    def test_session_config_fields(self) -> None:
        """Test that SessionConfig has expected fields."""
        config_fields = {f.name for f in fields(SessionConfig)}

        expected_fields = {
            "start_hail",
            "write_mode",
            "spark_uri",
            "hail_home",
            "extended_spark_conf",
            "use_enhanced_bgzip_codec",
            "output_partitions",
            "_target_",
        }

        for expected in expected_fields:
            assert expected in config_fields, f"Missing field: {expected}"

    def test_session_config_custom_values(self) -> None:
        """Test creating SessionConfig with custom values."""
        config = SessionConfig(
            start_hail=True,
            write_mode="overwrite",
            spark_uri="local[4]",
            output_partitions=100,
        )

        assert config.start_hail is True
        assert config.write_mode == "overwrite"
        assert config.spark_uri == "local[4]"
        assert config.output_partitions == 100

    def test_session_config_hail_home_default_is_none(self) -> None:
        """hail_home must default to None so it is resolved lazily by Session.

        Forces the CLI boot path to not pre-compute a hail-installation path,
        which removes the eager `import hail` from config module load.
        """
        config = SessionConfig()
        assert config.hail_home is None

    def test_session_config_hail_home_annotation_is_optional_str(self) -> None:
        """hail_home must be typed as `str | None` (not a bare `str`)."""
        annotations = {f.name: f.type for f in fields(SessionConfig)}
        assert annotations["hail_home"] == (str | None)

    def test_session_config_importable_without_hail(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SessionConfig must be importable when hail is not installed.

        Simulates an environment without hail by blocking the `hail` module in
        sys.modules and forcing a fresh import of gentropy.config. If the module
        still eagerly imports hail at load time, this raises ImportError.
        """

        class _MissingHailFinder:
            def find_module(self, name: str, path: object = None) -> None:
                if name == "hail" or name.startswith("hail."):
                    raise ImportError(f"simulated: hail not installed ({name})")

        # Drop any cached hail / gentropy.config modules so the reload runs
        # the module body again under the patched import system.
        for mod_name in list(sys.modules):
            if mod_name == "hail" or mod_name.startswith("hail."):
                monkeypatch.delitem(sys.modules, mod_name, raising=False)
        monkeypatch.delitem(sys.modules, "gentropy.config", raising=False)
        monkeypatch.setattr(sys, "meta_path", [_MissingHailFinder(), *sys.meta_path])

        reloaded = importlib.import_module("gentropy.config")

        assert hasattr(reloaded, "SessionConfig")
        assert reloaded.SessionConfig().hail_home is None


class TestStepConfig:
    """Test StepConfig base class."""

    def test_step_config_is_dataclass(self) -> None:
        """Test that StepConfig is a dataclass."""
        assert is_dataclass(StepConfig)

    def test_step_config_has_defaults(self) -> None:
        """Test that StepConfig has default defaults."""
        config = StepConfig(session=SessionConfig())
        assert config.session is not None

    def test_step_config_fields(self) -> None:
        """Test that StepConfig has expected fields."""
        config_fields = {f.name for f in fields(StepConfig)}

        expected_fields = {"session", "defaults"}

        for expected in expected_fields:
            assert expected in config_fields, f"Missing field: {expected}"


class TestColocalisationConfig:
    """Test ColocalisationConfig."""

    def test_colocalisation_config_is_dataclass(self) -> None:
        """Test that ColocalisationConfig is a dataclass."""
        assert is_dataclass(ColocalisationConfig)

    def test_colocalisation_config_inherits_from_step_config(self) -> None:
        """Test that ColocalisationConfig inherits from StepConfig."""
        assert issubclass(ColocalisationConfig, StepConfig)

    def test_colocalisation_config_has_required_fields(self) -> None:
        """Test that ColocalisationConfig has expected fields."""
        config_fields = {f.name for f in fields(ColocalisationConfig)}

        expected_fields = {
            "credible_set_path",
            "coloc_path",
            "colocalisation_method",
        }

        for expected in expected_fields:
            assert expected in config_fields, f"Missing field: {expected}"


class TestBiosampleIndexConfig:
    """Test BiosampleIndexConfig."""

    def test_biosample_index_config_is_dataclass(self) -> None:
        """Test that BiosampleIndexConfig is a dataclass."""
        assert is_dataclass(BiosampleIndexConfig)

    def test_biosample_index_config_inherits_from_step_config(self) -> None:
        """Test that BiosampleIndexConfig inherits from StepConfig."""
        assert issubclass(BiosampleIndexConfig, StepConfig)

    def test_biosample_index_config_has_required_fields(self) -> None:
        """Test that BiosampleIndexConfig has expected fields."""
        config_fields = {f.name for f in fields(BiosampleIndexConfig)}

        expected_fields = {
            "cell_ontology_input_path",
            "uberon_input_path",
            "efo_input_path",
            "biosample_index_path",
        }

        for expected in expected_fields:
            assert expected in config_fields, f"Missing field: {expected}"


def test_register_config() -> None:
    """Test that register_config can be called without errors."""
    # This just verifies the function doesn't raise exceptions
    try:
        register_config()
    except Exception as e:
        pytest.fail(f"register_config raised unexpected exception: {e}")


def test_config_class_exists() -> None:
    """Test that Config class exists and is a dataclass."""
    assert is_dataclass(Config)
