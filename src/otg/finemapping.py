"""Step to run study locus fine-mapping."""

from __future__ import annotations

from dataclasses import dataclass

from otg.common.session import Session
from otg.config import FinemappingStepConfig
from otg.method.finemapping import Finemapping


@dataclass
class FinemappingStep(FinemappingStepConfig):
    """Fine-mapping for an input locus"""

    session: Session = Session()

    def run(self: FinemappingStep) -> None:
        """Run Finemapping study table ingestion step."""
        self.session.logger.info(self.fm_filtered_StudyLocus_path)
        self.session.logger.info(self.fm_filtered_LDMatrix_path)
        self.session.logger.info(self.finemapped_locus_out)

        self.session.logger.info(self.fm_filtered_LDMatrix_out)
        self.session.logger.info(self.fm_filtered_StudyLocus_out)

        LDMatrix_file_paths = [f"{self.fm_filtered_LDMatrix_path}*.snappy.parquet"]
        fm_filtered_LDMatrix = self.session.read.parquet(*LDMatrix_file_paths)

        StudyLocus_file_paths = [f"{self.fm_filtered_StudyLocus_path}*.snappy.parquet"]
        fm_filtered_StudyLocus = self.session.read.parquet(*StudyLocus_file_paths)

        fm_filtered_Locus_outliers = Finemapping.outlier_detection(
            fm_filtered_StudyLocus,
            outlier_method="DENTIST",  # outlier_detection with susie will always be DENTIST
        )
        finemapped_locus = Finemapping.run_susie(
            fm_filtered_Locus_outliers,
            fm_filtered_LDMatrix,
            n_sample,  # need to get sample size for the study locus
            lead_SNP_ID,  # need lead SNP ID for study locus
        )

        # Write the output.
        finemapped_locus.df.write.mode(self.session.write_mode).parquet(
            self.finemapped_locus_out
        )
