"""Test that all modules can be imported. This ensures the lack of major compatibility issues."""

import importlib
import pkgutil

import gentropy


def test_all_modules_are_importable() -> None:
    """Test that all modules in the gentropy package can be imported."""
    import_errors = []

    def try_import(module_name: str) -> None:
        """Given a module name, attempt to import it."""
        try:
            importlib.import_module(module_name)
        except ImportError as e:
            import_errors.append(f"{module_name}: {str(e)}")

    # Iterate over all modules in Gentropy
    package = gentropy
    for _, name, _ in pkgutil.walk_packages(package.__path__, f"{package.__name__}."):
        try_import(name)
    if import_errors:
        raise AssertionError(
            "The following modules failed to import:\n" + "\n".join(import_errors)
        )
