"""Tests asserting the L2G-optional dependency contract (Phase B).

These tests pin the public contract of the ``[l2g]`` extra:

- ``gentropy.common.imports.install_hint`` produces a user-facing message
  that names the ``l2g`` extra and the pip-install command.
- Importing an L2G-bound module without the L2G stack surfaces an
  ``ImportError`` whose message contains the install-hint substring.
- Importing ``gentropy.l2g`` (the top-level step module) SUCCEEDS in the
  same sandboxed environment because heavy L2G imports are lazy.
- The four non-training step classes import cleanly without the L2G
  stack and can be inspected (no instantiation required, since they all
  do real work in ``__init__``).
"""

from __future__ import annotations

import importlib
import inspect
import sys

import pytest

L2G_HEAVY_PACKAGES = {
    "xgboost",
    "wandb",
    "shap",
    "sklearn",
    "skops",
    "huggingface_hub",
    "matplotlib",
}


class TestInstallHintForL2G:
    """The shared ``install_hint`` helper must support the l2g extra."""

    def test_install_hint_mentions_l2g(self) -> None:
        """The hint names the requested extra."""
        from gentropy.common.imports import install_hint

        message = install_hint("l2g")
        assert "l2g" in message

    def test_install_hint_includes_pip_command(self) -> None:
        """The hint includes the exact pip-install command for the extra."""
        from gentropy.common.imports import install_hint

        message = install_hint("l2g")
        assert "pip install gentropy[l2g]" in message


class _MissingL2GFinder:
    """A ``meta_path`` finder that raises on any L2G-heavy package import."""

    def find_spec(self, name: str, path: object = None, target: object = None) -> None:
        """Raise when the import system asks for any L2G-heavy package."""
        root = name.split(".", maxsplit=1)[0]
        if root in L2G_HEAVY_PACKAGES:
            raise ImportError(f"simulated: {root} not installed ({name})")

    def find_module(self, name: str, path: object = None) -> None:
        """Legacy hook retained for Python's older import machinery."""
        root = name.split(".", maxsplit=1)[0]
        if root in L2G_HEAVY_PACKAGES:
            raise ImportError(f"simulated: {root} not installed ({name})")


def _purge_l2g_and_module(module_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop L2G packages and the target module from ``sys.modules`` so reimport runs."""
    to_drop = []
    for cached in list(sys.modules):
        root = cached.split(".", maxsplit=1)[0]
        if root in L2G_HEAVY_PACKAGES:
            to_drop.append(cached)
    # Also purge the gentropy modules whose import binds to the L2G stack so
    # they are re-imported through the simulated-missing finder.
    for cached in list(sys.modules):
        if cached.startswith("gentropy.method.l2g") or cached in {
            "gentropy.dataset.l2g_prediction",
            "gentropy.l2g",
        }:
            to_drop.append(cached)
    for cached in to_drop:
        monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.delitem(sys.modules, module_name, raising=False)


class TestL2GBoundModuleGuard:
    """L2G-bound modules must surface the install hint."""

    @pytest.mark.parametrize(
        "module_name",
        [
            "gentropy.method.l2g.model",
            "gentropy.method.l2g.trainer",
            "gentropy.dataset.l2g_prediction",
        ],
    )
    def test_l2g_bound_module_raises_with_hint(
        self, module_name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Importing an L2G-bound module without the L2G stack raises ImportError with hint."""
        _purge_l2g_and_module(module_name, monkeypatch)
        monkeypatch.setattr(sys, "meta_path", [_MissingL2GFinder(), *sys.meta_path])

        with pytest.raises(ImportError) as excinfo:
            importlib.import_module(module_name)

        assert "pip install gentropy[l2g]" in str(excinfo.value)


class TestL2GStepModuleStaysLazy:
    """``gentropy.l2g`` must import cleanly without the L2G heavy stack."""

    def test_l2g_step_module_imports_without_l2g(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``gentropy.l2g`` imports because heavy deps live inside step methods."""
        _purge_l2g_and_module("gentropy.l2g", monkeypatch)
        monkeypatch.setattr(sys, "meta_path", [_MissingL2GFinder(), *sys.meta_path])

        mod = importlib.import_module("gentropy.l2g")

        # Sanity check: the four non-training step classes are exported.
        assert hasattr(mod, "LocusToGeneFeatureMatrixStep")
        assert hasattr(mod, "LocusToGeneTrainTestSplitStep")
        assert hasattr(mod, "LocusToGeneEvidenceStep")
        assert hasattr(mod, "LocusToGeneAssociationsStep")
        # The training step is also defined (its body imports lazily).
        assert hasattr(mod, "LocusToGeneStep")

    @pytest.mark.parametrize(
        "class_name",
        [
            "LocusToGeneFeatureMatrixStep",
            "LocusToGeneTrainTestSplitStep",
            "LocusToGeneEvidenceStep",
            "LocusToGeneAssociationsStep",
        ],
    )
    def test_non_training_step_classes_are_inspectable(
        self, class_name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The four non-training step classes have inspectable signatures."""
        _purge_l2g_and_module("gentropy.l2g", monkeypatch)
        monkeypatch.setattr(sys, "meta_path", [_MissingL2GFinder(), *sys.meta_path])

        mod = importlib.import_module("gentropy.l2g")
        cls = getattr(mod, class_name)
        # ``inspect.signature`` would fail if the class definition were
        # corrupted by a missing-import at class-construction time, so this
        # both proves the class loads and that its ``__init__`` is intact.
        sig = inspect.signature(cls.__init__)
        assert "session" in sig.parameters
