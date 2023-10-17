"""Step to run study locus fine-mapping."""

from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import DentistStepConfig
from otg.method.dentist import Dentist


@dataclass
class DentistStep(DentistStepConfig):
    """DENTIST outlier detection for an input locus"""

    session: Session = Session()

    def run(self: DentistStep) -> None:
        """Run DENTIST outlier detection step."""
        self.session.logger.info(self.fm_filtered_StudyLocus_path)
        self.session.logger.info(self.fm_filtered_StudyLocus_out)

        StudyLocus_file_paths = [f"{self.fm_filtered_StudyLocus_path}*.snappy.parquet"]
        fm_filtered_StudyLocus = self.session.read.parquet(*StudyLocus_file_paths)

        fm_filtered_Locus_outliers = Dentist.calculate_dentist(
            fm_filtered_StudyLocus,
        )

        # Write the output.
        fm_filtered_Locus_outliers.df.write.mode(self.session.write_mode).parquet(
            self.fm_filtered_StudyLocus_out
        )
