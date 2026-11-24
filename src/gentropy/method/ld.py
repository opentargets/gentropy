"""Performing linkage disequilibrium (LD) operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column


class LDAnnotator:
    """Class to annotate linkage disequilibrium (LD) operations from GnomAD."""

    @staticmethod
    def _get_major_population(ordered_populations: Column) -> Column:
        """Get major population based on an ldPopulationStructure array ordered by relativeSampleSize.

        If there is a tie for the major population, nfe is selected if it is one of the major populations.
        The first population in the array is selected if there is no tie for the major population, or there is a tie but nfe is not one of the major populations.

        Args:
            ordered_populations (Column): ldPopulationStructure array ordered by relativeSampleSize

        Returns:
            Column: major population
        """
        major_population_size = ordered_populations["relativeSampleSize"][0]
        major_populations = f.filter(
            ordered_populations,
            lambda x: x["relativeSampleSize"] == major_population_size,
        )
        # Check if nfe (Non-Finnish European) is one of the major populations
        has_nfe = f.filter(major_populations, lambda x: x["ldPopulation"] == "nfe")
        return f.when(
            (f.size(major_populations) > 1) & (f.size(has_nfe) == 1), f.lit("nfe")
        ).otherwise(ordered_populations["ldPopulation"][0])
