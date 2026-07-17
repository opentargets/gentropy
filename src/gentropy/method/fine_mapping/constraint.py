"""Definitions of per-study constraints for fine-mapping methods."""

from __future__ import annotations

from typing import ClassVar, Protocol

import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

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
            StructType: The schema of the constraint result.
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
            name (str): The name of the constraint.
            exp (Column): The expression representing the constraint.

        Returns:
            ConstraintResult: The result of applying the constraint.
        """
        return ConstraintResult(
            _df=si.df.select(
                "studyId",
                f.array(
                    f.struct(
                        f.lit(self.name).alias("name"), self.expression.alias("value")
                    ).alias("constraints"),
                ),
            )
        )


class IsAllowedStudyType(MethodConstraint):
    """Class representing the constraint for allowed study types."""

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
    """Class representing the constraint for the MultiSuSiE method."""

    name = "hasSumstats"

    def __init__(self) -> None:
        """Initialize the constraint for the MultiSuSiE method."""
        self.expression = f.col("hasSumstats") == f.lit(True)


class PassSumstatQC(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method."""

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
    """Class representing the constraint for the MultiSuSiE method."""

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


class HasAllowedMajorAncestry(MethodConstraint):
    """Class representing the constraint for single & allowed ancestry for SuSiE based methods."""

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
