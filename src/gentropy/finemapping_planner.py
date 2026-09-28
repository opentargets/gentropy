"""Module for generating fine mapping plans."""

from functools import reduce

from gentropy import Session, StudyIndex
from gentropy.method.fine_mapping import FineMappingConstraintRegistry


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
        registry = FineMappingConstraintRegistry().registry

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
