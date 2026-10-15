"""Fine mapping planner dataset module.

This module captures the schema for the dataset that represents the availability of executing the fine-mapping methods for each study.
The dataset unique key is (studyId, route), where studyId represents the single study identifier from StudyIndex and route
represents a single fine-mapping method route (e.g. `MultiSuSiE`).

The eligibility of the study to undergo the
fine-mapping under a specific route is defined by the constraints that have to be satisfied for that study or combination of studies.

This module contains two datasets:

- `FineMappingPlanner`: The dataset that captures the eligibility of each study to undergo the fine-mapping under a specific route.
- `FineMappingManifest`: The dataset that builds on the `FineMappingPlanner` with only eligible study combinations under each route,
    used as an entry point for the fine-mapping pipeline.
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

    This is the intermediate dataset that is a result of the fine-mapping constraint resolution over the `gentropy.dataset.study_index.StudyIndex` dataset.
    The dataset captures the eligibility of each study to undergo the fine-mapping under a specific route.

    This dataset is later used as input to generate the `eligible` study sets for each fine-mapping route
    by generating the `gentropy.dataset.fine_mapping.FineMappingManifest` dataset.


    Note that this dataset contains all available study x route combinations (including eligible and ineligible ones).

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


    Schema definition:
    - runId: Unique identifier for the fine-mapping run.
    - studyId: Unique identifier for the study.
    - route: The fine-mapping method route (e.g. `MultiSuSiE`).
    - constraints: A list of constraints that determine the eligibility of the study for fine-mapping
      under the specified route. Each constraint is represented as a struct with two fields:
        - name: The name of the constraint.
        - value: A boolean indicating whether the constraint is satisfied (True) or not (False).
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


class FineMappingManifest(Dataset):
    """Class representing a manifest for fine-mapping.

    This dataset captures the availability of summary statistics for each study and fine-mapping route.
    The dataset shall be used as an entry point for the fine-mapping pipelines.

    Each fine-mapping run shall be uniquely identified by the `runId` and `route` fields. The fine-mapping process
    shall start from collecting all studies (summary statistics) that belong to a single `runId` and
    then execute the fine-mapping method for each study and route combination.

    Table cardinality is unique per (`studyId`, `route`), each study can be eligible to more than one
    fine-mapping route (e.g. MultiSuSiE and SuSiE-inf) and will appear once per eligible route.

    Below one can find an example of the fine-mapping manifest dataset for two studies that undergo
    single fine-mapping run (run1) under the MultiSuSiE route. The run shall be treated as multi-study (multi ancestry),
    hence both studies have the same trait set ([trait1]) and different major ancestries (EUR and AFR).

    Examples:
    ---
    >>> data = [("run1", "study1", "multi_susie_route", "path/to/summary_stats1", "EUR", ["trait1"], 1000),
    ...         ("run1", "study2", "multi_susie_route", "path/to/summary_stats2", "AFR", ["trait1"], 2000)]
    >>> df = spark.createDataFrame(data, schema=FineMappingManifest.get_schema())
    >>> manifest = FineMappingManifest(df)
    >>> assert isinstance(manifest, FineMappingManifest)
    >>> manifest.df.show(truncate=False)
    +-----+-------+-----------------+----------------------+-------------+------------------------+-------------------+
    |runId|studyId|route            |summarystatsLocation  |majorAncestry|traitFromSourceMappedIds|effectiveSampleSize|
    +-----+-------+-----------------+----------------------+-------------+------------------------+-------------------+
    |run1 |study1 |multi_susie_route|path/to/summary_stats1|EUR          |[trait1]                |1000               |
    |run1 |study2 |multi_susie_route|path/to/summary_stats2|AFR          |[trait1]                |2000               |
    +-----+-------+-----------------+----------------------+-------------+------------------------+-------------------+
    <BLANKLINE>


    Schema definition:
    - runId: Unique identifier for the fine-mapping run.
    - studyId: Unique identifier for the study.
    - route: The fine-mapping method route (e.g. multi_susie_route).
    - summarystatsLocation: The location of the summary statistics file for the study.
    - majorAncestry: The major ancestry of the study population.
    - traitFromSourceMappedIds: A list of traits associated with the study.
    - effectiveSampleSize: The effective sample size of the study.
    """

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Get the schema of the fine-mapping manifest.

        Returns:
            t.StructType: The schema of the fine-mapping manifest.
        """
        return t.StructType(
            [
                t.StructField("runId", t.StringType(), nullable=False),
                t.StructField("studyId", t.StringType(), nullable=False),
                t.StructField("route", t.StringType(), nullable=False),
                t.StructField("summarystatsLocation", t.StringType(), nullable=False),
                t.StructField("majorAncestry", t.StringType(), nullable=False),
                t.StructField(
                    "traitFromSourceMappedIds",
                    t.ArrayType(t.StringType(), containsNull=False),
                    nullable=False,
                ),
                t.StructField("effectiveSampleSize", t.IntegerType(), nullable=False),
            ]
        )
