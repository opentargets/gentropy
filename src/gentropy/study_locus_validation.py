"""Step to validate study locus dataset against study index."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus


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
        gwas_significance: float,
        output_path: str,
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            study_index_path (str): Path to study index file.
            study_locus_path (list[str]): Path to study locus dataset.
            gwas_significance (float): GWAS significance threshold.
            output_path (str): Path to write the output file.
        """
        # Reading datasets:
        study_index = StudyIndex.from_parquet(session, study_index_path)

        # Running validation then writing output:
        (
            StudyLocus.from_parquet(session, list(study_locus_path))
            .validate_lead_pvalue(
                pvalue_cutoff=gwas_significance
            )  # Flagging study locus with subsignificant p-values
            .validate_study(study_index)  # Flagging studies not in study index
            .validate_unique_study_locus_id()  # Flagging duplicated study locus ids
            .df.write.parquet(output_path)
        )
