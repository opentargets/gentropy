"""This module defines the inputs for SuShie fine-mapping steps."""

from pyspark.sql import functions as f

from gentropy import Session, StudyIndex, StudyLocus
from gentropy.common.types import UKBBLDAncestryEnum


class SuShiELdInputStep:
    """Step to prepare LD matrix chunks for SuShiE fine-mapping."""

    def __init__(
        self,
        session: Session,
        sushie_sumstat_input_path: str,
        ld_block_matrix_path: str,
        ld_slice_output_path: str,
        ancestry: str,
    ) -> None:
        session.logger.info(f"LD matrix path: {ld_block_matrix_path}")
        session.logger.info(f"LD slice output path: {ld_slice_output_path}")
        session.logger.info(f"SuShiE sumstat input path: {sushie_sumstat_input_path}")
        session.logger.info(f"Ancestry: {ancestry}")


class SuShiESumStatInputStep:
    """Step to prepare summary statistics for SuShiE fine-mapping."""

    def __init__(
        self,
        session: Session,
        clumped_study_locus_path: str,
        study_index_path: str,
        sushie_sumstat_output_path: str,
    ) -> None:
        """This step prepares summary statistics for SuShiE fine-mapping.

        (1) Read study index
        (2) Find studies with relative sample size of 1 (single ancestry)
        """
        ancestries = [
            UKBBLDAncestryEnum.CENTRAL_SOUTH_ASIAN,
            UKBBLDAncestryEnum.AFRICAN,
            UKBBLDAncestryEnum.EUROPEAN,
        ]

        studies = StudyIndex.from_parquet(
            session, study_index_path
        ).filter_single_ancestry_studies(ancestries)

        (
            (
                StudyLocus.from_parquet(session, clumped_study_locus_path)
                .find_duplicates()
                .to_sushie_input(studies)
            )
            .write.partitionBy("leadVariantId", "studyId", "ancestry")
            .csv(sushie_sumstat_output_path)
        )
        # Make sure we keep only the loci where study is in 2 or 3 divergent ancestries.
