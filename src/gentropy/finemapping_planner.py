"""Module for generating fine mapping plans."""

from functools import reduce

from gentropy import Session, StudyIndex
from gentropy.common.types import LDPopulation
from gentropy.dataset.study_index import StudyAnalysisFlag, StudyQualityCheck
from gentropy.method.fine_mapping.constraint_set import (
    ConstraintSet,
    MultiSuSiEConstraintSet,
)


class FineMappingPlanGeneratorStep:
    """Step generating a fine-mapping plan from a study index."""

    def __init__(self, session: Session, input_path: str, output_path: str) -> None:
        """Resolve every registered constraint set against the study index and write the combined plan.

        Args:
            session (Session): Session object.
            input_path (str): Path to the input study index.
            output_path (str): Path to write the combined fine-mapping plan to, partitioned by route.
        """
        study_index = StudyIndex.from_parquet(session, input_path)

        registry: dict[str, ConstraintSet] = {
            "MultiSuSiE": MultiSuSiEConstraintSet(
                disallowed_flags=[
                    StudyAnalysisFlag.MULTIVARIATE_ANALYSIS,
                    StudyAnalysisFlag.EXWAS,
                    StudyAnalysisFlag.NON_ADDITIVE,
                    StudyAnalysisFlag.WGS_WAS,
                    StudyAnalysisFlag.GXG,
                    StudyAnalysisFlag.GxE,
                    StudyAnalysisFlag.CASE_CASE_STUDY,
                ],
                allowed_ancestries=[
                    LDPopulation.NFE,
                    LDPopulation.AFR,
                    LDPopulation.CSA,
                ],
                disallowed_reasons=[
                    StudyQualityCheck.FAILED_GC_LAMBDA_CHECK,
                    StudyQualityCheck.FAILED_MEAN_BETA_CHECK,
                    StudyQualityCheck.FAILED_PZ_CHECK,
                    StudyQualityCheck.NO_OT_CURATION,
                    StudyQualityCheck.SMALL_NUMBER_OF_SNPS,
                ],
                relative_sample_size_threshold=0.95,
            ),
        }

        self.plans = {
            method_name: constraint_set.resolve(study_index)
            for method_name, constraint_set in registry.items()
        }

        (
            reduce(lambda p1, p2: p1 + p2, self.plans.values())
            .df.coalesce(1)
            .sortWithinPartitions("route", "runId", "studyId")
            .write.mode(session.write_mode)
            .partitionBy("route")
            .parquet(output_path)
        )
