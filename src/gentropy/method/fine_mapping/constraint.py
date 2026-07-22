"""Definitions of per-study constraints for fine-mapping methods.

The constraints are defined as classes that implement the MethodConstraint protocol.
Each constraint class has a `name` class variable and `expression` instance variable that defines how the constraint is
evaluated on the StudyIndex dataset.

The constraints are self-contained and can be used independently to mark studies as eligible for a specific fine-mapping method route.
Callers evaluate a constraint by creating an instance (passing any required parameters to the constructor) and reading its
`name`/`expression` directly, typically to build a single combined `.select()` over a StudyIndex rather than resolving each
constraint separately.
"""

from __future__ import annotations

from typing import ClassVar, Protocol

import pyspark.sql.types as t
from pyspark.sql import Column
from pyspark.sql import functions as f

from gentropy.common.spark import order_array_of_structs_by_field
from gentropy.common.types import LDPopulation
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_index import (
    StudyAnalysisFlag,
    StudyQualityCheck,
    StudyType,
)
from gentropy.method.ld import LDAnnotator


class ConstraintResult(Dataset):
    """Class representing the result of applying a constraint to a StudyIndex dataset."""

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Get the schema of the constraint result.

        Returns:
            t.StructType: The schema of the constraint result.
        """
        return t.StructType(
            [
                t.StructField("studyId", t.StringType(), nullable=False),
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


class MethodConstraint(Protocol):
    """Class representing the unique constraint on applying fine-mapping methods to a study."""

    name: ClassVar[str]
    """Class variable representing the name of the constraint."""
    expression: Column
    """Instance variable representing the expression to evaluate the constraint."""


class IsAllowedStudyType(MethodConstraint):
    """Class representing the constraint for allowed study types.

    Examples:
        >>> data = [("s1", "p", "gwas"), ("s2", "p", "eqtl")]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING"
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = IsAllowedStudyType(allowed_study_types=[StudyType.GWAS])
        >>> si.df.select("studyId", constraint.expression.alias("isAllowedStudyType")).show(truncate=False)
        +-------+------------------+
        |studyId|isAllowedStudyType|
        +-------+------------------+
        |s1     |true              |
        |s2     |false             |
        +-------+------------------+
        <BLANKLINE>
    """

    name = "isAllowedStudyType"

    def __init__(self, allowed_study_types: list[StudyType]) -> None:
        """Initialize the constraint for allowed study types.

        Args:
            allowed_study_types (list[StudyType]): The list of allowed study types.
        """
        self.expression = f.col("studyType").isin(
            [s.value for s in allowed_study_types]
        )


class HasSumstats(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method.

    Examples:
        >>> data = [("s1", "p", "gwas", True), ("s2", "p", "gwas", False)]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, hasSumstats BOOLEAN"
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = HasSumstats()
        >>> si.df.select("studyId", constraint.expression.alias("hasSumstats")).show(truncate=False)
        +-------+-----------+
        |studyId|hasSumstats|
        +-------+-----------+
        |s1     |true       |
        |s2     |false      |
        +-------+-----------+
        <BLANKLINE>
    """

    name = "hasSumstats"

    def __init__(self) -> None:
        """Initialize the constraint for the MultiSuSiE method."""
        self.expression = f.col("hasSumstats") == f.lit(True)


class PassSumstatQC(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method.

    Examples:
        >>> data = [("s1", "p", "gwas", []), ("s2", "p", "gwas", [StudyQualityCheck.UNRESOLVED_TARGET.value]), ("s3", "p", "gwas", None)]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, qualityControls ARRAY<STRING>"
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = PassSumstatQC(disallowed_reasons=[StudyQualityCheck.UNRESOLVED_TARGET])
        >>> si.df.select("studyId", constraint.expression.alias("passSumstatQC")).orderBy("studyId").show(truncate=False)
        +-------+-------------+
        |studyId|passSumstatQC|
        +-------+-------------+
        |s1     |true         |
        |s2     |false        |
        |s3     |false        |
        +-------+-------------+
        <BLANKLINE>
    """

    name = "passSumstatQC"

    def __init__(self, disallowed_reasons: list[StudyQualityCheck]) -> None:
        """Initialize the constraint for the MultiSuSiE method.

        The constraint checks for:

        1. The `qualityControls` column is not null.
        2. The `qualityControls` column does not contain any of the disallowed reasons.

        If any of the reasons is inside the `qualityControls` column or the column is null (indicating no qc performed),
            the study is marked as not passing the constraint.

        Args:
            disallowed_reasons (list[StudyQualityCheck]): The list of disallowed quality check reasons.
        """
        self.expression = (
            ~f.arrays_overlap(
                f.coalesce(f.col("qualityControls"), f.array()),
                f.array([f.lit(reason.value) for reason in disallowed_reasons]),
            )
            & f.col("qualityControls").isNotNull()
        )


class HasAllowedAnalysisFlags(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method.

    Examples:
        >>> data = [("s1", "p", "gwas", []), ("s2", "p", "gwas", [StudyAnalysisFlag.CASE_CASE_STUDY.value])]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, analysisFlags ARRAY<STRING>"
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = HasAllowedAnalysisFlags(disallowed_flags=[StudyAnalysisFlag.CASE_CASE_STUDY])
        >>> si.df.select("studyId", constraint.expression.alias("hasAllowedAnalysisFlags")).show(truncate=False)
        +-------+-----------------------+
        |studyId|hasAllowedAnalysisFlags|
        +-------+-----------------------+
        |s1     |true                   |
        |s2     |false                  |
        +-------+-----------------------+
        <BLANKLINE>
    """

    name = "hasAllowedAnalysisFlags"

    def __init__(self, disallowed_flags: list[StudyAnalysisFlag]) -> None:
        """Initialize the constraint for the MultiSuSiE method.

        The constraint checks for the following:

        1. The `analysisFlags` column does not contain any of the disallowed flags.

        If any of the flags is inside the `analysisFlags` column, the study is marked as not passing the constraint.

        Args:
            disallowed_flags (list[StudyAnalysisFlag]): The list of disallowed analysis flags.

        """
        self.expression = ~f.arrays_overlap(
            f.col("analysisFlags"),
            f.array([f.lit(flag.value) for flag in disallowed_flags]),
        )


class HasMappedTrait(MethodConstraint):
    """Class representing the constraint requiring at least one mapped trait.

    If the `traitFromSourceMappedIds` column is empty or null, the study is marked as not passing the constraint.

    Examples:
        >>> data = [("s1", "p", "gwas", ["EFO_1"]), ("s2", "p", "gwas", [])]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, traitFromSourceMappedIds ARRAY<STRING>"
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = HasMappedTrait()
        >>> si.df.select("studyId", constraint.expression.alias("hasMappedTrait")).show(truncate=False)
        +-------+--------------+
        |studyId|hasMappedTrait|
        +-------+--------------+
        |s1     |true          |
        |s2     |false         |
        +-------+--------------+
        <BLANKLINE>
    """

    name = "hasMappedTrait"

    def __init__(self) -> None:
        """Initialize the constraint requiring at least one mapped trait."""
        self.expression = (
            f.size(f.coalesce(f.col("traitFromSourceMappedIds"), f.array())) > 0
        )


class HasAllowedMajorAncestry(MethodConstraint):
    """Class representing the constraint for single & allowed ancestry for SuSiE based methods.

    If the major ancestry (the one with the highest relative sample size) is not in the allowed ancestries or its relative sample size is below the threshold,
        the study is marked as not passing the constraint.

    Examples:
        >>> data = [
        ...     ("s1", "p", "gwas", [("nfe", 0.8), ("afr", 0.2)]),
        ...     ("s2", "p", "gwas", [("nfe", 0.4), ("afr", 0.6)]),
        ...     ("s3", "p", "gwas", [("eas", 0.9)]),
        ... ]
        >>> schema = (
        ...     "studyId STRING, projectId STRING, studyType STRING, "
        ...     "ldPopulationStructure ARRAY<STRUCT<ldPopulation:STRING,relativeSampleSize:DOUBLE>>"
        ... )
        >>> from gentropy.dataset.study_index import StudyIndex
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = HasAllowedMajorAncestry(allowed_ancestries=[LDPopulation.NFE], relative_sample_size_threshold=0.5)
        >>> si.df.select("studyId", constraint.expression.alias("hasAllowedMajorAncestry")).orderBy("studyId").show(truncate=False)
        +-------+-----------------------+
        |studyId|hasAllowedMajorAncestry|
        +-------+-----------------------+
        |s1     |true                   |
        |s2     |false                  |
        |s3     |false                  |
        +-------+-----------------------+
        <BLANKLINE>
    """

    name = "hasAllowedMajorAncestry"

    def __init__(
        self,
        allowed_ancestries: list[LDPopulation],
        relative_sample_size_threshold: float,
    ) -> None:
        """Initialize the constraint for single & allowed ancestry for SuSiE based methods.

        Args:
            allowed_ancestries (list[LDPopulation]): The list of allowed ancestries.
            relative_sample_size_threshold (float): The threshold for relative sample size.

        """
        ld_exists = f.col("ldPopulationStructure").isNotNull() & (
            f.size(f.col("ldPopulationStructure")) > 0
        )
        ld_pops_sorted = order_array_of_structs_by_field(
            "ldPopulationStructure", "relativeSampleSize"
        )
        major_anc = f.when(
            ld_pops_sorted.isNotNull(),
            LDAnnotator._get_major_population(ld_pops_sorted),
        )
        major_above_threshold = (
            # In case of tie, we can just pick the first one.
            ld_pops_sorted.getItem(0).getField("relativeSampleSize")
            >= relative_sample_size_threshold
        )
        major_allowed = major_anc.isin([a.value for a in allowed_ancestries])

        self.expression = ld_exists & major_above_threshold & major_allowed
