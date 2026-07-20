"""Sets of fine-mapping constraints composed into sets for methods."""

from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

from pyspark.sql import DataFrame, Window
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
    HasMappedTrait,
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
        """Initialize the constraint set for the MultiSuSiE method.

        Args:
            allowed_ancestries (list[LDPopulation]): The list of allowed ancestries.
            relative_sample_size_threshold (float): The threshold for relative sample size.
            disallowed_reasons (list[StudyQualityCheck]): The list of disallowed quality check reasons.
            disallowed_flags (list[StudyAnalysisFlag]): The list of disallowed analysis flags.
        """
        self.constraints: list[MethodConstraint] = [
            IsAllowedStudyType(allowed_study_types=[StudyType.GWAS]),
            HasSumstats(),
            HasMappedTrait(),
            PassSumstatQC(disallowed_reasons=disallowed_reasons),
            HasAllowedAnalysisFlags(disallowed_flags=disallowed_flags),
            HasAllowedMajorAncestry(
                allowed_ancestries=allowed_ancestries,
                relative_sample_size_threshold=relative_sample_size_threshold,
            ),
        ]

        self.route = FineMappingRoute.MULTI_SUSIE_ROUTE

    def _resolve_constraints(self, si: StudyIndex) -> ConstraintResult:
        """Evaluate every constraint's expression on the study index in a single pass.

        Each constraint's own `annotate()` still works standalone (used and tested
        independently), but here we read each constraint's `name`/`expression` directly
        and build one combined `.select()` instead of resolving each constraint via its
        own `annotate()` call and unioning the results - this avoids Spark scheduling one
        independent scan of the (cached) study index per constraint.

        Args:
            si (StudyIndex): The input StudyIndex.

        Returns:
            ConstraintResult: One row per studyId with every constraint's result.
        """
        merged_df = si.df.select(
            "studyId",
            f.array(
                *[
                    f.struct(
                        f.lit(constraint.name).alias("name"),
                        constraint.expression.alias("value"),
                    )
                    for constraint in self.constraints
                ]
            ).alias("constraints"),
        )
        return ConstraintResult(_df=merged_df)

    @staticmethod
    def _compute_n_eff(df: DataFrame) -> DataFrame:
        """Compute the effective sample size, using case-control formula when applicable.

        Args:
            df (DataFrame): Study-level dataframe with qualityControls, nCases, nControls, nSamples.

        Returns:
            DataFrame: Same dataframe with an added n_eff column.
        """
        return df.withColumn(
            "n_eff",
            f.when(
                f.array_contains(
                    f.col("qualityControls"),
                    StudyQualityCheck.CASE_CONTROL_STUDY_DESIGN.value,
                ),
                effective_sample_size(f.col("nCases"), f.col("nControls")),
            ).otherwise(f.col("nSamples")),
        )

    @staticmethod
    def _compute_major_ancestry(df: DataFrame) -> DataFrame:
        """Derive the major (highest relative sample size) LD ancestry population per study.

        Args:
            df (DataFrame): Study-level dataframe with an ldPopulationStructure column.

        Returns:
            DataFrame: Same dataframe with an added majorAncestry column.
        """
        return df.withColumn(
            "majorAncestry",
            order_array_of_structs_by_field(
                "ldPopulationStructure", "relativeSampleSize"
            )
            .getItem(0)
            .getField("ldPopulation"),
        )

    def _compute_representative_selection_inputs(self, si: StudyIndex) -> DataFrame:
        """Build the traitSet/n_eff/majorAncestry branch used for representative-study selection.

        This branch and `_resolve_constraints` both only depend on the (already cached)
        study index, not on each other, so `resolve()` runs them concurrently.

        Args:
            si (StudyIndex): The input StudyIndex.

        Returns:
            DataFrame: Study-level dataframe with traitSet, n_eff, majorAncestry columns.
        """
        return (
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
            .transform(self._compute_n_eff)
            .transform(self._compute_major_ancestry)
        )

    @staticmethod
    def _join_eligibility(
        df: DataFrame, all_constraints: ConstraintResult
    ) -> DataFrame:
        """Join the merged constraints onto the study dataframe and derive overall eligibility.

        Args:
            df (DataFrame): Study-level dataframe with a studyId column.
            all_constraints (ConstraintResult): Per-study merged constraints.

        Returns:
            DataFrame: Study-level dataframe with constraints and isElligible columns added.
        """
        return df.join(
            all_constraints.df.withColumn(
                "isElligible",
                all_struct_fields_in_array(f.col("constraints"), "value", f.lit(True)),
            ),
            on="studyId",
            how="left",
        )

    @staticmethod
    def _select_representative_studies(df: DataFrame) -> DataFrame:
        """Flag, per (traitSet, majorAncestry), the eligible study with the highest n_eff.

        An ineligible study is never selected as representative, even when it is the
        sole (ineligible) study within its trait/ancestry group.

        Args:
            df (DataFrame): Study-level dataframe with traitSet, majorAncestry, isElligible, n_eff columns.

        Returns:
            DataFrame: Same dataframe with an added representativeStudy column.
        """
        representative_study_window = (
            Window()
            .partitionBy(
                f.col("traitSet"),
                f.col("majorAncestry"),
                f.col("isElligible"),
            )
            .orderBy(f.col("n_eff").desc())
        )
        return df.withColumn(
            "representativeStudy",
            (f.row_number().over(representative_study_window) == 1)
            & f.col("isElligible"),
        )

    @staticmethod
    def _compute_has_other_ancestry_counterpart(df: DataFrame) -> DataFrame:
        """Flag representative studies that share their traitSet with another ancestry's representative.

        Args:
            df (DataFrame): Study-level dataframe with traitSet, majorAncestry, representativeStudy columns.

        Returns:
            DataFrame: Same dataframe with an added hasOtherAncestryCounterpart column.
        """
        representative_study_set_window = Window().partitionBy(
            f.col("traitSet"), f.col("representativeStudy")
        )
        return df.withColumn(
            "hasOtherAncestryCounterpart",
            f.when(
                f.col("representativeStudy"),
                f.size(
                    f.collect_set("majorAncestry").over(representative_study_set_window)
                )
                > 1,
            ).otherwise(f.lit(False)),
        )

    @staticmethod
    def _append_derived_constraints(df: DataFrame) -> DataFrame:
        """Fold representativeStudy and hasOtherAncestryCounterpart into the constraints array.

        Args:
            df (DataFrame): Study-level dataframe with constraints, representativeStudy, hasOtherAncestryCounterpart columns.

        Returns:
            DataFrame: Same dataframe with the two derived flags merged into constraints.
        """
        return df.withColumn(
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

    @staticmethod
    def _assign_run_id(df: DataFrame) -> DataFrame:
        """Assign a shared, deterministic runId to every study in a multi-ancestry run.

        Studies without a cross-ancestry counterpart for their trait get a null runId.
        Studies that share a trait and are all representative of different ancestries
        get the same runId, derived from the sorted set of their studyIds.

        Note:
            The final runId digest (`collect_list("studyId").over(Window().partitionBy("runId"))`)
            is only computed for rows with a non-null runId. Evaluating it over the full
            dataframe would put every null-runId study (the majority, since most studies
            have no cross-ancestry counterpart) into one giant window partition, forcing
            Spark to materialize the full collect_list per row in that partition.

        Args:
            df (DataFrame): Study-level dataframe with traitSet, majorAncestry, hasOtherAncestryCounterpart, studyId columns.

        Returns:
            DataFrame: Same dataframe with an added runId column.
        """
        with_provisional_run_id = (
            df.withColumn("id", f.monotonically_increasing_id())
            .withColumn(
                "runId",
                f.when(
                    f.col("hasOtherAncestryCounterpart"),
                    f.first("id").over(
                        Window().partitionBy("traitSet").orderBy("majorAncestry")
                    ),
                ).otherwise(f.lit(None)),
            )
            # Persisted because both branches below filter this same dataframe -
            # without persisting, each filter would recompute the full upstream
            # lineage (join, representative-study selection, etc.) independently.
            .persist()
        )

        without_run_id = with_provisional_run_id.filter(f.col("runId").isNull())
        with_run_id = with_provisional_run_id.filter(
            f.col("runId").isNotNull()
        ).withColumn(
            "runId",
            f.concat_ws(
                ",",
                f.array_sort(
                    f.array_distinct(
                        f.collect_list("studyId").over(Window().partitionBy("runId"))
                    )
                ),
            ),
        )

        return without_run_id.unionByName(with_run_id)

    def resolve(self, si: StudyIndex) -> FineMappingPlanner:
        """Resolve all of the constrains on the dataframe and return a new dataframe.

        Args:
            si (StudyIndex): The input StudyIndex.

        Returns:
            FineMappingPlanner: A FineMappingPlanner dataset containing the allowed studies.

        Raises:
            ValueError: If the number of distinct studies in the output plan doesn't match the input.
        """
        si.persist()
        # Force the study index cache to materialize now, before any downstream branch
        # is built. Without this, downstream branches can race to populate the same
        # cache concurrently, each triggering its own independent parquet scan instead
        # of reusing one materialized cache. This also gives us the input study count
        # for the sanity check below.
        input_study_count = si.df.select("studyId").distinct().count()

        # _resolve_constraints and the representative-selection inputs (traitSet/n_eff/
        # majorAncestry, via validate_ccs()) only depend on the cached si above, not on
        # each other - Spark's own scheduler does not parallelize independent branches
        # like these on its own (verified: only one stage is ever active at a time on a
        # dataset this size), so each branch is built AND materialized (persist + count)
        # inside its own thread, letting both Spark jobs run concurrently.
        def _build_and_materialize_constraints() -> ConstraintResult:
            result = self._resolve_constraints(si)
            result.persist()
            result.df.count()
            return result

        def _build_and_materialize_selection_inputs() -> DataFrame:
            result = self._compute_representative_selection_inputs(si).persist()
            result.count()
            return result

        with ThreadPoolExecutor(max_workers=2) as executor:
            constraints_future = executor.submit(_build_and_materialize_constraints)
            selection_inputs_future = executor.submit(
                _build_and_materialize_selection_inputs
            )
            all_constraints = constraints_future.result()
            selection_inputs = selection_inputs_future.result()

        df = (
            selection_inputs.transform(
                lambda df: self._join_eligibility(df, all_constraints)
            )
            .select(
                "studyId",
                "traitSet",
                "majorAncestry",
                "n_eff",
                "isElligible",
                "constraints",
            )
            .transform(self._select_representative_studies)
            .transform(self._compute_has_other_ancestry_counterpart)
            .transform(self._append_derived_constraints)
            .transform(self._assign_run_id)
            .select(
                "studyId",
                "runId",
                "constraints",
                f.lit(FineMappingRoute.MULTI_SUSIE_ROUTE.value).alias("route"),
            )
            # Persisted so the sanity-check count() below and the caller's eventual
            # write both reuse this single materialization, instead of each
            # recomputing the full pipeline from scratch.
            .persist()
        )

        output_study_count = df.select("studyId").distinct().count()
        if output_study_count != input_study_count:
            raise ValueError(
                "Fine-mapping plan resolution changed the number of distinct studies: "
                f"input had {input_study_count}, output has {output_study_count}."
            )

        return FineMappingPlanner(_df=df)
