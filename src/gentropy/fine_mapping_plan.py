"""Module for generating fine mapping plans."""

from functools import reduce

from gentropy import Session, StudyIndex
from gentropy.method.fine_mapping.constraints.model import ConstraintSet
from gentropy.method.fine_mapping.constraints.multisusie import MultiSuSiEConstraintSet
from gentropy.method.fine_mapping.constraints.pics import PICSConstraintSet
from gentropy.method.fine_mapping.constraints.susieinf import SuSiEInfConstraintSet


class FineMappingPlanGeneratorStep:
    def __init__(self, session: Session, input_path: str, output_path: str) -> None:
        study_index = StudyIndex.from_parquet(session, input_path)

        registry: dict[str, ConstraintSet] = {
            "MultiSuSiE": MultiSuSiEConstraintSet(),
            "SuSiE-inf": SuSiEInfConstraintSet(),
            "PICS": PICSConstraintSet(),
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
