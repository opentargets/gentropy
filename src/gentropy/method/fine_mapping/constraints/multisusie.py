"""Module for multisusie constraints."""

from gentropy.dataset.fine_mapping import FineMappingPlanner, FineMappingRoute
from gentropy.dataset.study_index import StudyIndex
from gentropy.method.fine_mapping.constraints.common import (
    HasAllowedAnalysisFlags,
    HasAllowedAncestry,
    HasSumstats,
    IsGwasStudyType,
    PassSumstatQC,
)
from gentropy.method.fine_mapping.constraints.model import ConstraintSet


class MultiSuSiEConstraintSet(ConstraintSet):
    """Class representing a set of constraints for the MultiSuSiE method."""

    def __init__(self):
        self.constraints = [
            IsGwasStudyType(),
            HasSumstats(),
            HasAllowedAncestry(
                allowed_ancestries=["EUR", "CSA", "AFR"],
                relative_sample_size_threshold=0.95,
                multi_ancestry=True,
            ),
            PassSumstatQC(allowed_reasons=[]),
            HasAllowedAnalysisFlags(allowed_flags=[]),
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
