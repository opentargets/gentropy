"""Trait for the quality control flags a dataset carries.

``QualityControls`` holds the flags raised on each row, as an array of strings.
The column must be present; a row with no flags holds an empty array or null.

The trait does not say which flags are allowed. Each dataset has its own flag
enumeration, and the staging and released datasets keep theirs separate, so the
enumeration belongs with the dataset and the functions that raise the flags.

Example:
    >>> QualityControls.quality_controls.dtype.simpleString()
    'array<string>'
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class QualityControls(Trait):
    """Holds the quality control flags raised on each row."""

    quality_controls = Field(t.ArrayType(t.StringType(), containsNull=False))
