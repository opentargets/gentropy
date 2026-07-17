"""Module for multisusie constraints."""

from gentropy.common.types import LDPopulation
from gentropy.dataset.fine_mapping import FineMappingPlanner, FineMappingRoute
from gentropy.dataset.study_index import (
    StudyAnalysisFlag,
    StudyIndex,
    StudyQualityCheck,
    StudyType,
)
from gentropy.method.fine_mapping.constraints.common import (
    HasAllowedAnalysisFlags,
    HasAllowedAncestry,
    HasSumstats,
    IsAllowedStudyType,
    PassSumstatQC,
)
from gentropy.method.fine_mapping.constraints.model import ConstraintSet


class MultiSuSiEConstraintSet(ConstraintSet):
    """Class representing a set of constraints for the MultiSuSiE method."""

    def __init__(
        self,
        allowed_ancestries: list[LDPopulation],
        relative_sample_size_threshold: float,
        multi_ancestry: bool,
        disallowed_reasons: list[StudyQualityCheck],
        disallowed_flags: list[StudyAnalysisFlag],
    ):
        self.constraints = [
            IsAllowedStudyType(allowed_study_types=[StudyType.GWAS]),
            HasSumstats(),
            HasAllowedAncestry(
                allowed_ancestries=allowed_ancestries,
                relative_sample_size_threshold=relative_sample_size_threshold,
                multi_ancestry=multi_ancestry,
            ),
            PassSumstatQC(disallowed_reasons=disallowed_reasons),
            HasAllowedAnalysisFlags(disallowed_flags=disallowed_flags),
        ]

        self.route = FineMappingRoute.MULTI_SUSIE_ROUTE

    def resolve(self, si: StudyIndex) -> FineMappingPlanner:
        """Resolve all of the constrains on the dataframe and return a new dataframe.

        Args:
            si (StudyIndex): The input StudyIndex.

        Returns:
            FineMappingPlanner: A FineMappingPlanner dataset containing the allowed studies.
        """
        df = si.df
        for constraint in self.constraints:
            df = constraint.apply(df)
        return FineMappingPlanner(df)
