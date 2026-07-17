"""Sets of fine-mapping constraints composed into sets for methods."""

from typing import Protocol

from pyspark.sql import Window
from pyspark.sql import functions as f

from gentropy.common.spark import (
    all_struct_fields_in_array,
    order_array_of_structs_by_field,
)
from gentropy.common.stats import effective_sample_size
from gentropy.common.types import LDPopulation
from gentropy.dataset.fine_mapping import FineMappingPlanner, FineMappingRoute
from gentropy.dataset.study_index import (
    StudyAnalysisFlag,
    StudyIndex,
    StudyQualityCheck,
    StudyType,
)
from gentropy.method.fine_mapping.constraint import (
    ConstraintResult,
    HasAllowedAnalysisFlags,
    HasAllowedMajorAncestry,
    HasSumstats,
    IsAllowedStudyType,
    MethodConstraint,
    PassSumstatQC,
)


class ConstraintSet(Protocol):
    """Class representing a set of constraints for the methods of fine-mapping."""

    def resolve(self, si: StudyIndex) -> FineMappingPlanner:
        """Resolve all of the constrains on the dataframe and return a new dataframe.

        Args:
            si (StudyIndex): The input dataframe.

        Returns:
            FineMappingPlanner: Dataset with the combinations of the allowed studies for given constrain set.
        """
        # Set the initial flag as True
        raise NotImplementedError(
            "The resolve method should be implemented in the subclasses of FineMappingConstraintSet."
        )


class MultiSuSiEConstraintSet(ConstraintSet):
    """Class representing a set of constraints for the MultiSuSiE method."""

    def __init__(
        self,
        allowed_ancestries: list[LDPopulation],
        relative_sample_size_threshold: float,
        disallowed_reasons: list[StudyQualityCheck],
        disallowed_flags: list[StudyAnalysisFlag],
    ):
        self.constraints: list[MethodConstraint] = [
            IsAllowedStudyType(allowed_study_types=[StudyType.GWAS]),
            HasSumstats(),
            PassSumstatQC(disallowed_reasons=disallowed_reasons),
            HasAllowedAnalysisFlags(disallowed_flags=disallowed_flags),
            HasAllowedMajorAncestry(
                allowed_ancestries=allowed_ancestries,
                relative_sample_size_threshold=relative_sample_size_threshold,
            ),
        ]

        self.route = FineMappingRoute.MULTI_SUSIE_ROUTE

    def resolve(self, si: StudyIndex) -> FineMappingPlanner:
        """Resolve all of the constrains on the dataframe and return a new dataframe.

        Args:
            si (StudyIndex): The input StudyIndex.

        Returns:
            FineMappingPlanner: A FineMappingPlanner dataset containing the allowed studies.
        """
        si.persist()
        # Map each constraint to resolve ConstraintResult
        resolved_constraints = [
            constraint.annotate(si).df for constraint in self.constraints
        ]
        from functools import reduce

        # UnionByName on all resolved constraints
        all_constraints = ConstraintResult(
            _df=reduce(lambda x, y: x.unionByName(y), resolved_constraints)
            .groupBy("studyId")
            .agg(f.collect_list("constraints").alias("constraints"))
            .select(
                "studyId",
                f.array_distinct(f.flatten(f.col("constraints"))).alias("constraints"),
            )
            .persist()
        )

        representative_study = (
            Window()
            .partitionBy(
                f.col("traitSet"),
                f.col("majorAncestry"),
                f.col("isElligible"),
            )
            .orderBy(f.col("n_eff").desc())
        )
        representative_study_set = Window().partitionBy(
            f.col("traitSet"), f.col("representativeStudy")
        )

        df = (
            si.validate_ccs()
            .df.select(
                "studyId",
                "traitFromSourceMappedIds",
                "ldPopulationStructure",
                "qualityControls",
                "nSamples",
                "nCases",
                "nControls",
            )
            .withColumn("traitSet", f.array_distinct(f.col("traitFromSourceMappedIds")))
            .withColumn(
                "n_eff",
                f.when(
                    f.array_contains(
                        f.col("qualityControls"),
                        StudyQualityCheck.CASE_CONTROL_STUDY_DESIGN.value,
                    ),
                    effective_sample_size(f.col("nCases"), f.col("nControls")),
                ).otherwise(f.col("nSamples")),
            )
            .withColumn(
                "majorAncestry",
                order_array_of_structs_by_field(
                    "ldPopulationStructure", "relativeSampleSize"
                )
                .getItem(0)
                .getField("ldPopulation"),
            )
            .join(
                all_constraints.df.withColumn(
                    "isElligible",
                    all_struct_fields_in_array(
                        f.col("constraints"), "value", f.lit(True)
                    ),
                ),
                on="studyId",
                how="left",
            )
            .select(
                "studyId",
                "traitSet",
                "majorAncestry",
                "n_eff",
                "isElligible",
                "constraints",
            )
            .withColumn(
                "representativeStudy", f.row_number().over(representative_study) == 1
            )
            .withColumn(
                "hasOtherAncestryCounterpart",
                f.when(
                    f.col("representativeStudy"),
                    f.size(
                        f.collect_set("majorAncestry").over(representative_study_set)
                    )
                    > 1,
                ).otherwise(f.lit(False)),
            )
            .withColumn(
                "constraints",
                f.array_union(
                    f.array(
                        f.struct(
                            f.lit("representativeStudy").alias("name"),
                            f.col("representativeStudy").alias("value"),
                        ),
                        f.struct(
                            f.lit("hasOtherAncestryCounterpart").alias("name"),
                            f.col("hasOtherAncestryCounterpart").alias("value"),
                        ),
                    ),
                    f.col("constraints"),
                ),
            )
            # Add runId over traitSet for representative studies
            .withColumn("id", f.monotonically_increasing_id())
            .withColumn(
                "runId",
                f.when(
                    f.col("hasOtherAncestryCounterpart"),
                    f.first("id").over(
                        Window().partitionBy("traitSet").orderBy("majorAncestry")
                    ),
                ).otherwise(f.lit(None)),
            )
            .select(
                "id",
                "studyId",
                "runId",
                "constraints",
                f.lit(FineMappingRoute.MULTI_SUSIE_ROUTE.value).alias("route"),
            )
        )

        return FineMappingPlanner(_df=df)
