"""Model representing the constraint for the methods of fine-mapping."""

from typing import Protocol

from pyspark.sql import DataFrame

from gentropy.dataset.fine_mapping import FineMappingPlanner
from gentropy.dataset.study_index import StudyIndex


class MethodConstraint(Protocol):
    """Class representing the constraint for the methods of fine-mapping."""

    def apply(self, df: DataFrame) -> DataFrame:
        """Filter out rows that do not satisfy the constraint and return a new dataframe.

        Args:
            df (DataFrame): The input dataframe.

        Returns:
            DataFrame: The dataframe with an additional column indicating whether the constraint is satisfied.
        """
        raise NotImplementedError(
            "The apply method should be implemented in the subclasses of FineMappingMethodConstraint."
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
