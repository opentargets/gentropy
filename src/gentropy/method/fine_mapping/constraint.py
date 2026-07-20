"""Definitions of per-study constraints for fine-mapping methods."""

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
    StudyIndex,
    StudyQualityCheck,
    StudyType,
)


class ConstraintResult(Dataset):
    """Class representing the result of applying a constraint to a dataset."""

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
    expression: Column

    def annotate(self, si: StudyIndex) -> ConstraintResult:
        """Apply the constraint to the StudyIndex and return the result.

        Args:
            si (StudyIndex): The input StudyIndex.

        Returns:
            ConstraintResult: The result of applying the constraint.
        """
        return ConstraintResult(
            _df=si.df.select(
                "studyId",
                f.array(
                    f.struct(
                        f.lit(self.name).alias("name"), self.expression.alias("value")
                    )
                ).alias("constraints"),
            )
        )


class IsAllowedStudyType(MethodConstraint):
    """Class representing the constraint for allowed study types.

    Examples:
        >>> data = [("s1", "p", "gwas"), ("s2", "p", "eqtl")]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING"
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = IsAllowedStudyType(allowed_study_types=[StudyType.GWAS])
        >>> constraint.annotate(si).df.show(truncate=False)
        +-------+-----------------------------+
        |studyId|constraints                  |
        +-------+-----------------------------+
        |s1     |[{isAllowedStudyType, true}] |
        |s2     |[{isAllowedStudyType, false}]|
        +-------+-----------------------------+
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
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> HasSumstats().annotate(si).df.show(truncate=False)
        +-------+----------------------+
        |studyId|constraints           |
        +-------+----------------------+
        |s1     |[{hasSumstats, true}] |
        |s2     |[{hasSumstats, false}]|
        +-------+----------------------+
        <BLANKLINE>
    """

    name = "hasSumstats"

    def __init__(self) -> None:
        """Initialize the constraint for the MultiSuSiE method."""
        self.expression = f.col("hasSumstats") == f.lit(True)


class PassSumstatQC(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method.

    Examples:
        >>> data = [("s1", "p", "gwas", []), ("s2", "p", "gwas", [StudyQualityCheck.UNRESOLVED_TARGET.value])]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, qualityControls ARRAY<STRING>"
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = PassSumstatQC(disallowed_reasons=[StudyQualityCheck.UNRESOLVED_TARGET])
        >>> constraint.annotate(si).df.show(truncate=False)
        +-------+------------------------+
        |studyId|constraints             |
        +-------+------------------------+
        |s1     |[{passSumstatQC, true}] |
        |s2     |[{passSumstatQC, false}]|
        +-------+------------------------+
        <BLANKLINE>
    """

    name = "passSumstatQC"

    def __init__(self, disallowed_reasons: list[StudyQualityCheck]) -> None:
        """Initialize the constraint for the MultiSuSiE method.

        Args:
            disallowed_reasons (list[StudyQualityCheck]): The list of disallowed quality check reasons.
        """
        self.expression = ~f.arrays_overlap(
            f.col("qualityControls"),
            f.array([f.lit(reason.value) for reason in disallowed_reasons]),
        )


class HasAllowedAnalysisFlags(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method.

    Examples:
        >>> data = [("s1", "p", "gwas", []), ("s2", "p", "gwas", [StudyAnalysisFlag.CASE_CASE_STUDY.value])]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, analysisFlags ARRAY<STRING>"
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = HasAllowedAnalysisFlags(disallowed_flags=[StudyAnalysisFlag.CASE_CASE_STUDY])
        >>> constraint.annotate(si).df.show(truncate=False)
        +-------+----------------------------------+
        |studyId|constraints                       |
        +-------+----------------------------------+
        |s1     |[{hasAllowedAnalysisFlags, true}] |
        |s2     |[{hasAllowedAnalysisFlags, false}]|
        +-------+----------------------------------+
        <BLANKLINE>
    """

    name = "hasAllowedAnalysisFlags"

    def __init__(self, disallowed_flags: list[StudyAnalysisFlag]) -> None:
        """Initialize the constraint for the MultiSuSiE method.

        Args:
            disallowed_flags (list[StudyAnalysisFlag]): The list of disallowed analysis flags.
        """
        self.expression = ~f.arrays_overlap(
            f.col("analysisFlags"),
            f.array([f.lit(flag.value) for flag in disallowed_flags]),
        )


class HasMappedTrait(MethodConstraint):
    """Class representing the constraint requiring at least one mapped trait.

    Examples:
        >>> data = [("s1", "p", "gwas", ["EFO_1"]), ("s2", "p", "gwas", [])]
        >>> schema = "studyId STRING, projectId STRING, studyType STRING, traitFromSourceMappedIds ARRAY<STRING>"
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> HasMappedTrait().annotate(si).df.show(truncate=False)
        +-------+-------------------------+
        |studyId|constraints              |
        +-------+-------------------------+
        |s1     |[{hasMappedTrait, true}] |
        |s2     |[{hasMappedTrait, false}]|
        +-------+-------------------------+
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
        >>> si = StudyIndex(_df=spark.createDataFrame(data, schema))
        >>> constraint = HasAllowedMajorAncestry(allowed_ancestries=[LDPopulation.NFE], relative_sample_size_threshold=0.5)
        >>> constraint.annotate(si).df.orderBy("studyId").show(truncate=False)
        +-------+----------------------------------+
        |studyId|constraints                       |
        +-------+----------------------------------+
        |s1     |[{hasAllowedMajorAncestry, true}] |
        |s2     |[{hasAllowedMajorAncestry, false}]|
        |s3     |[{hasAllowedMajorAncestry, false}]|
        +-------+----------------------------------+
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
        major_anc = order_array_of_structs_by_field(
            "ldPopulationStructure", "relativeSampleSize"
        ).getItem(0)

        major_above_threshold = (
            major_anc.getField("relativeSampleSize") >= relative_sample_size_threshold
        )
        major_allowed = major_anc.getField("ldPopulation").isin(
            [a.value for a in allowed_ancestries]
        )

        self.expression = major_above_threshold & major_allowed
