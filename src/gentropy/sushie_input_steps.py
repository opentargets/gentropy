"""This module defines the inputs for SuShie fine-mapping steps."""

from pyspark.sql import functions as f

from gentropy import Session, StudyIndex, StudyLocus
from gentropy.common.types import UKBBLDAncestryEnum


class SuShiELdInputStep:
    """Step to prepare LD matrix chunks for SuShiE fine-mapping."""

    def __init__(
        self,
        ld_matrix_paths: dict[str, str],
    ) -> None:
        pass


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

        clumped_study_locus = (
            StudyLocus.from_parquet(session, clumped_study_locus_path)
            .find_duplicates()
            .to_sushie_input(studies)
        )
        # Make sure we keep only the loci where study is in 2 or 3 divergent ancestries.
