"""Shared helpers for guarding imports behind optional dependency extras."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def install_hint(extra: str) -> str:
    """Return an actionable install message for an optional extra.

    Args:
        extra (str): Name of the optional extra (e.g. ``"hail"``).

    Returns:
        str: User-facing message suggesting the pip-install command.
    """
    return (
        f"This functionality requires the `{extra}` extra. "
        f"Install with: `pip install gentropy[{extra}]`"
    )


@contextmanager
def optional_imports(extra: str) -> Iterator[None]:
    """Re-raise ``ImportError`` inside the block with an install hint.

    Wrap ``import`` statements for symbols provided by an optional extra so
    that, when the extra is not installed, the user sees an actionable
    message instead of a bare ``ImportError``. The literal ``import`` lines
    remain visible inside the ``with`` block, so static type checkers resolve
    the imported symbols normally.

    Example:
        >>> with optional_imports("hail"):
        ...     import hail as hl  # doctest: +SKIP

    Args:
        extra (str): Name of the optional extra (e.g. ``"hail"``, ``"l2g"``).

    Yields:
        None: No value is yielded; the context manager exists purely for its
            exception-translation side effect. Use ``with optional_imports(...)``
            without an ``as`` clause.

    Raises:
        ImportError: If any ``import`` inside the block fails.
    """
    try:
        yield
    except ImportError as exc:
        raise ImportError(install_hint(extra)) from exc
