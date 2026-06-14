"""Tests asserting the hail-optional dependency contract (Phase A).

These tests pin the public contract of the ``[hail]`` extra:

- ``gentropy.common.imports.install_hint`` produces a user-facing message
  that names the extra and the pip-install command.
- Importing a hail-bound datasource module without hail surfaces an
  ``ImportError`` whose message contains that install-hint substring.
- ``utils.spark.get_spark_testing_conf`` honors the ``with_hail`` flag:
  hail-specific keys are absent when ``with_hail=False`` and present when
  ``with_hail=True``.
"""

from __future__ import annotations

import importlib
import sys

import pytest


class TestInstallHint:
    """Test the shared optional-extra helper."""

    def test_install_hint_module_importable(self) -> None:
        """``gentropy.common.imports`` is importable."""
        mod = importlib.import_module("gentropy.common.imports")
        assert hasattr(mod, "install_hint")

    def test_install_hint_mentions_extra(self) -> None:
        """The hint names the requested extra."""
        from gentropy.common.imports import install_hint

        message = install_hint("hail")
        assert "hail" in message

    def test_install_hint_includes_pip_command(self) -> None:
        """The hint includes the exact pip-install command for the extra."""
        from gentropy.common.imports import install_hint

        message = install_hint("hail")
        assert "pip install gentropy[hail]" in message


class _MissingHailFinder:
    """A ``meta_path`` finder that raises on any hail import."""

    def find_spec(self, name: str, path: object = None, target: object = None) -> None:
        """Raise when the import system asks for hail."""
        if name == "hail" or name.startswith("hail."):
            raise ImportError(f"simulated: hail not installed ({name})")

    def find_module(self, name: str, path: object = None) -> None:
        """Legacy hook retained for Python's older import machinery."""
        if name == "hail" or name.startswith("hail."):
            raise ImportError(f"simulated: hail not installed ({name})")


def _purge_hail_and_module(module_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop hail and the target module from ``sys.modules`` so reimport runs."""
    for cached in list(sys.modules):
        if cached == "hail" or cached.startswith("hail."):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.delitem(sys.modules, module_name, raising=False)


class TestHailBoundModuleGuard:
    """Hail-bound datasource modules must surface the install hint."""

    @pytest.mark.parametrize(
        "module_name",
        [
            "gentropy.datasource.gnomad.ld",
            "gentropy.datasource.gnomad.variants",
            "gentropy.datasource.finngen.finemapping",
            "gentropy.datasource.pan_ukbb_ld.ld",
        ],
    )
    def test_hail_bound_module_raises_with_hint(
        self, module_name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Importing a hail-bound module without hail raises ImportError with hint."""
        _purge_hail_and_module(module_name, monkeypatch)
        monkeypatch.setattr(sys, "meta_path", [_MissingHailFinder(), *sys.meta_path])

        with pytest.raises(ImportError) as excinfo:
            importlib.import_module(module_name)

        assert "pip install gentropy[hail]" in str(excinfo.value)


class TestSparkTestingConfHailFlag:
    """``get_spark_testing_conf`` must honor the ``with_hail`` flag."""

    def test_without_hail_omits_kryo_registrator(self) -> None:
        """``with_hail=False`` returns a conf without the HailKryoRegistrator."""
        from utils.spark import get_spark_testing_conf

        conf = get_spark_testing_conf(with_hail=False)
        debug = conf.toDebugString()
        assert "HailKryoRegistrator" not in debug
        assert "hail-all-spark.jar" not in debug

    def test_with_hail_includes_kryo_registrator(self) -> None:
        """``with_hail=True`` returns a conf carrying the HailKryoRegistrator."""
        from utils.spark import get_spark_testing_conf

        conf = get_spark_testing_conf(with_hail=True)
        debug = conf.toDebugString()
        assert "HailKryoRegistrator" in debug
        assert "hail-all-spark.jar" in debug
