"""Module for PICS constraints."""

from gentropy.dataset.fine_mapping import FineMappingPlanner, FineMappingRoute
from gentropy.dataset.study_index import StudyIndex
from gentropy.method.fine_mapping.constraints.common import IsGwasStudyType
from gentropy.method.fine_mapping.constraints.model import ConstraintSet


class PICSConstraintSet(ConstraintSet):
    """Class representing a set of constraints for the PICS method."""

    def __init__(self):
        self.constraints = [IsGwasStudyType()]

        self.route = FineMappingRoute.PICS_ROUTE

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
