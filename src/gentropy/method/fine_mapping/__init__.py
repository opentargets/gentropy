"""Module representing fine-mapping."""

from gentropy.common.types import LDPopulation
from gentropy.dataset.study_index import StudyAnalysisFlag, StudyQualityCheck
from gentropy.method.fine_mapping.constraint_set import (
    ConstraintSet,
    MultiSuSiEConstraintSet,
)


class FineMappingConstraintRegistry:
    """Registry of constraint sets for fine-mapping methods."""

    @property
    def registry(self) -> dict[str, ConstraintSet]:
        """Get the registry of constraint sets for fine-mapping methods.

        Returns:
            dict[str, ConstraintSet]: A dictionary mapping method names to their corresponding constraint sets.
        """
        return {
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
                    LDPopulation.EAS,
                ],
                disallowed_reasons=[
                    StudyQualityCheck.FAILED_GC_LAMBDA_CHECK,
                    StudyQualityCheck.FAILED_MEAN_BETA_CHECK,
                    StudyQualityCheck.FAILED_PZ_CHECK,
                    StudyQualityCheck.NO_OT_CURATION,
                    StudyQualityCheck.SMALL_NUMBER_OF_SNPS,
                    StudyQualityCheck.SUMSTATS_NOT_AVAILABLE,
                ],
                relative_sample_size_threshold=0.95,
            ),
        }


__all__ = ["FineMappingConstraintRegistry"]
