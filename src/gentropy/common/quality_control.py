"""Helpers for quality control flags."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

if TYPE_CHECKING:
    from enum import Enum

    from pyspark.sql import Column


def add_flag(quality_controls: Column, condition: Column, flag: Enum) -> Column:
    """Add ``flag`` to the quality controls of the rows where ``condition`` holds.

    The flag is written by its value. A row that already carries it keeps one
    copy, and a null ``qualityControls`` counts as no flags.

    Args:
        quality_controls (Column): The ``qualityControls`` column.
        condition (Column): Where to raise the flag; null leaves the row as it is.
        flag (Enum): The flag, from the dataset's own flag enumeration.

    Returns:
        Column: The quality controls, each flag at most once.

    Examples:
        >>> from enum import Enum
        >>> class Flag(Enum):
        ...     CLUMPED = "clumped"
        >>> df = spark.createDataFrame(
        ...     [(["a"], True), (None, True), (["clumped"], True), (["a"], False)],
        ...     "qc array<string>, hit boolean",
        ... )
        >>> flagged = add_flag(f.col("qc"), f.col("hit"), Flag.CLUMPED)
        >>> [row[0] for row in df.select(flagged).collect()]
        [['a', 'clumped'], ['clumped'], ['clumped'], ['a']]
    """
    return f.when(
        condition,
        f.array_union(
            f.coalesce(quality_controls, f.array().cast(t.ArrayType(t.StringType()))),
            f.array(f.lit(flag.value)),
        ),
    ).otherwise(quality_controls)
