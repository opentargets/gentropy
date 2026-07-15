"""Module for generating fine mapping plans."""

from functools import reduce

from gentropy import Session, StudyIndex
from gentropy.common.types import LDPopulation
from gentropy.dataset.study_index import StudyAnalysisFlag, StudyQualityCheck
from gentropy.method.fine_mapping.constraints.model import ConstraintSet
from gentropy.method.fine_mapping.constraints.multisusie import MultiSuSiEConstraintSet


class FineMappingPlanGeneratorStep:
    def __init__(self, session: Session, input_path: str, output_path: str) -> None:
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
                multi_ancestry=True,
            ),
        }

        self.plans = {
            method_name: constraint_set.resolve(study_index)
            for method_name, constraint_set in registry.items()
        }

        (
            reduce(lambda p1, p2: p1 + p2, self.plans.values())
            .df.repartition(session.output_partitions)
            .write.mode(session.write_mode)
            .partitionBy("route")
            .parquet(output_path)
        )
