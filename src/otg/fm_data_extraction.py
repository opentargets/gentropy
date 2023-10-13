"""Step for fine-mapping data extraction."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl

from otg.common.session import Session
from otg.config import FMDataExtractionStepConfig
from otg.fm_data_extraction import FMDataExtraction


@dataclass
class FMDataExtractionStep(FMDataExtractionStepConfig):
    """Fine-mapping data extraction step. Extracting LD Matrix and matching SNP with a locus for inputs into FM."""

    session: Session = Session()

    def run(self: FMDataExtractionStep) -> None:
        """Run FMDataExtraction - data extraction for fine-mapping step."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")
        self.session.logger.info(self.summary_statistics_path)
        self.session.logger.info(self.fm_filtered_LDMatrix_out)
        self.session.logger.info(self.fm_filtered_StudyLocus_out)

        # getting summary statistics from UKBB data in etl_playground (for testing example)
        file_paths = [f"{self.summary_statistics_path}*.snappy.parquet"]
        sumstats = self.session.read.parquet(*file_paths)
        lead_SNP_id, fm_study_locus = FMDataExtraction.get_FM_study_locus(sumstats)
        unfiltered_LDMatrix, SNP_ids_38 = FMDataExtraction.get_gnomad_ld_matrix(
            lead_SNP_id
        )
        self.session.logger.info("get_gnomad_ld_matrix() ran successfully")

        (
            fm_filtered_LDMatrix,
            fm_filtered_StudyLocus,
        ) = FMDataExtraction.get_matching_snps(
            unfiltered_LDMatrix, fm_study_locus, SNP_ids_38
        )
        self.session.logger.info("get_matching_snps() ran successfully")
        # Write the output.
        fm_filtered_LDMatrix.df.write.mode(self.session.write_mode).parquet(
            self.fm_filtered_LDMatrix_out
        )

        fm_filtered_StudyLocus.df.write.mode(self.session.write_mode).parquet(
            self.fm_filtered_StudyLocus_out
        )
