"""Fine mapping planner dataset module.

This module captures the schema for the dataset that represents the availablity of executing the fine-mapping methods for each study.
The dataset unique key is (studyId, route), where studyId represents the single study identifier from StudyIndex and route
represents a single fine-mapping method route (e.g. MultiSuSiE).

The eligibility of the study to undergo the
fine-mapping under a specific route is defined by the constraints that have to be satisfied for that study or combination of studies.
"""

from __future__ import annotations

from enum import StrEnum

from pyspark.sql import types as t

from gentropy.dataset.dataset import Dataset


class FineMappingRoute(StrEnum):
    """Enum representing the route of the fine-mapping method."""

    MULTI_SUSIE_ROUTE = "multi_susie_route"
    """Route for multi-SuSiE fine-mapping methods."""


class FineMappingPlanner(Dataset):
    """Class representing a planner for fine-mapping methods.

    Examples:
    ---
    >>> data = [("run1", "study1", "multi_susie_route", [("constraint1", True), ("constraint2", False)]),
    ...         ("run2", "study2", "multi_susie_route", [("constraint2", False),])]
    >>> df = spark.createDataFrame(data, schema=FineMappingPlanner.get_schema())
    >>> from gentropy.dataset.fine_mapping import FineMappingPlanner
    >>> planner = FineMappingPlanner(df)
    >>> assert isinstance(planner, FineMappingPlanner)
    >>> planner.df.show(truncate=False)
    +-----+-------+-----------------+-------------------------------------------+
    |runId|studyId|route            |constraints                                |
    +-----+-------+-----------------+-------------------------------------------+
    |run1 |study1 |multi_susie_route|[{constraint1, true}, {constraint2, false}]|
    |run2 |study2 |multi_susie_route|[{constraint2, false}]                     |
    +-----+-------+-----------------+-------------------------------------------+
    <BLANKLINE>
    """

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Get the schema of the planner.

        Returns:
            t.StructType: The schema of the planner.
        """
        return t.StructType(
            [
                t.StructField("runId", t.StringType(), nullable=True),
                t.StructField("studyId", t.StringType(), nullable=False),
                t.StructField("route", t.StringType(), nullable=False),
                t.StructField(
                    "constraints",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("name", t.StringType(), nullable=False),
                                t.StructField("value", t.BooleanType(), nullable=False),
                            ]
                        )
                    ),
                    nullable=False,
                ),
            ]
        )

    def __add__(self, other: FineMappingPlanner) -> FineMappingPlanner:
        """Combine two FineMappingPlanner datasets.

        The combined dataset is unique per (studyId, route), not per studyId alone: a
        study eligible for more than one fine-mapping route (e.g. both MultiSuSiE and
        SuSiE-inf) legitimately appears once per eligible route. Callers must not assume
        studyId is unique across the combined output.

        Args:
            other (FineMappingPlanner): Another FineMappingPlanner dataset to combine with.

        Returns:
            FineMappingPlanner: A new FineMappingPlanner dataset containing the combined data.
        """
        combined_df = self.df.unionByName(other.df)
        return FineMappingPlanner(combined_df)
