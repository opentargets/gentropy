"""Step to run clump associations from summary statistics or study locus."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.summary_statistics import SummaryStatistics


class ClumpMethod(Enum):
    """Method of clumping to use."""

    LD_BASED = "ld_based"
    WINDOW_BASED = "window_based"


@dataclass
class ClumpStep:
    """Clumping step with customizable method.

    Attributes:
        session (Session): Session object.
        clump_method (ClumpMethod): Method of clumping to use.
        study_index_path (str): Path to study index.
        ld_index_path (str): Path to LD index.
        clumped_study_locus_path (str): Output path for the clumped study locus dataset.
    """

    session: Session = MISSING
    clump_method: ClumpMethod = MISSING
    input_path: str = MISSING
    study_index_path: str = MISSING
    ld_index_path: str = MISSING
    clumped_study_locus_path: str = MISSING
    data: StudyLocus | SummaryStatistics = field(init=False)

    def __post_init__(self: ClumpStep) -> None:
        """Run the clumping step.

        Raises:
            ValueError: If clump method is not supported.
        """
        # Extract data by defining data type
        input_cols = self.session.spark.read.parquet(self.input_path).columns
        if "studyLocusId" in input_cols:
            self.data = StudyLocus.from_parquet(self.session, self.input_path)
        else:
            self.data = SummaryStatistics.from_parquet(self.session, self.input_path)

        # Transform
        if self.clump_method.value not in [
            ClumpMethod.LD_BASED.value,
            ClumpMethod.WINDOW_BASED.value,
        ]:
            raise ValueError(f"Clump method {self.clump_method} not supported.")

        elif self.clump_method == ClumpMethod.WINDOW_BASED and isinstance(
            self.data, SummaryStatistics
        ):
            clumped_study_locus = self.data.window_based_clumping()

        elif self.clump_method == ClumpMethod.LD_BASED and isinstance(
            self.data, StudyLocus
        ):
            ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)
            study_index = StudyIndex.from_parquet(self.session, self.study_index_path)

            clumped_study_locus = self.data.annotate_ld(
                study_index=study_index, ld_index=ld_index
            ).clump()

        # Load
        clumped_study_locus.df.write.mode(self.session.write_mode).parquet(
            self.clumped_study_locus_path
        )
