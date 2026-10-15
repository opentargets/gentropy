"""Helpers for linkage disequilibrium."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column


def major_population(structure: Column) -> Column:
    """The population with the largest share of a study's samples.

    A tie goes to ``nfe`` when it is among the largest, since the LD panel is
    largest for it; otherwise to the first of the tied populations by name, so
    the choice does not depend on the order the study lists them in.

    Args:
        structure (Column): ``ldPopulationStructure``, as (ldPopulation,
            relativeSampleSize) structs.

    Returns:
        Column: The population's name; null when the structure is null or empty.

    Examples:
        >>> df = spark.createDataFrame(
        ...     [([("afr", 0.8), ("nfe", 0.2)],), ([("afr", 0.5), ("nfe", 0.5)],),
        ...      ([("eas", 0.5), ("afr", 0.5)],), (None,)],
        ...     "s array<struct<ldPopulation: string, relativeSampleSize: double>>",
        ... )
        >>> [row[0] for row in df.select(major_population(f.col("s"))).collect()]
        ['afr', 'nfe', 'afr', None]
    """
    largest = f.array_max(f.transform(structure, lambda p: p["relativeSampleSize"]))
    tied = f.transform(
        f.filter(structure, lambda p: p["relativeSampleSize"] == largest),
        lambda p: p["ldPopulation"],
    )
    return f.when(f.array_contains(tied, "nfe"), f.lit("nfe")).otherwise(
        f.array_min(tied)
    )
