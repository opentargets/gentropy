"""Shared helpers for guarding imports behind optional dependency extras."""

from __future__ import annotations


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
