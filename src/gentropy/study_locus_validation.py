"""Step to validate study locus dataset against study index."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus


class StudyLocusValidationStep:
    """Study index validation step.

    This step reads and outputs a study index dataset with flagged studies
    when target of disease validation fails.
    """

    def __init__(
        self,
        session: Session,
        study_index_path: str,
        study_locus_path: list[str],
        valid_study_locus_path: str,
        invalid_study_locus_path: str,
        invalid_qc_reasons: list[str] = [],
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            study_index_path (str): Path to study index file.
            study_locus_path (list[str]): Path to study locus dataset.
            valid_study_locus_path (str): Path to write the valid records.
            invalid_study_locus_path (str): Path to write the output file.
            invalid_qc_reasons (list[str]): List of invalid quality check reason names from `StudyLocusQualityCheck` (e.g. ['SUBSIGNIFICANT_FLAG']).
        """
        # Reading datasets:
        study_index = StudyIndex.from_parquet(session, study_index_path)

        # Running validation then writing output:
        study_locus_with_qc = (
            StudyLocus.from_parquet(session, list(study_locus_path))
            # Add flag for MHC region
            .qc_MHC_region()
            .validate_study(study_index)  # Flagging studies not in study index
            .annotate_study_type(study_index)  # Add study type to study locus
            .qc_redundant_top_hits_from_PICS()  # Flagging top hits from studies with PICS summary statistics
            .qc_explained_by_SuSiE()  # Flagging credible sets in regions explained by SuSiE
            # Annotates credible intervals and filter to only keep 99% credible sets
            .filter_credible_set(credible_interval=CredibleInterval.IS99)
            # Annotate credible set confidence:
            .assign_confidence()
        ).persist()  # we will need this for 2 types of outputs

        study_locus_with_qc.valid_rows(invalid_qc_reasons, invalid=True).df.write.mode(
            session.write_mode
        ).parquet(invalid_study_locus_path)

        study_locus_with_qc.valid_rows(invalid_qc_reasons).df.write.mode(
            session.write_mode
        ).parquet(valid_study_locus_path)
