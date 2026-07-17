"""Fine mapping planner dataset module."""

from __future__ import annotations

from enum import StrEnum

from pyspark.sql import types as t

from gentropy.dataset.dataset import Dataset


class FineMappingRoute(StrEnum):
    """Enum representing the route of the fine-mapping method."""

    SUSIE_INF_ROUTE = "susie_inf_route"
    """Route for SuSiE inference fine-mapping methods."""

    PICS_ROUTE = "pics_route"
    """Route for PICS fine-mapping methods."""

    MULTI_SUSIE_ROUTE = "multi_susie_route"
    """Route for multi-SuSiE fine-mapping methods."""


class FineMappingPlanner(Dataset):
    """Class representing a planner for fine-mapping methods."""

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Get the schema of the planner.

        Returns:
            StructType: The schema of the planner.
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

        Args:
            other (FineMappingPlanner): Another FineMappingPlanner dataset to combine with.

        Returns:
            FineMappingPlanner: A new FineMappingPlanner dataset containing the combined data.
        """
        combined_df = self.df.unionByName(other.df)
        return FineMappingPlanner(combined_df)
