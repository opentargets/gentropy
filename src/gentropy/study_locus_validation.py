"""Step to validate study locus dataset against study index."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.dataset.target_index import TargetIndex


class StudyLocusValidationStep:
    """Study index validation step.

    This step reads and outputs a study index dataset with flagged studies
    when target of disease validation fails.
    """

    def __init__(
        self,
        session: Session,
        study_locus_path: list[str],
        study_index_path: str,
        target_index_path: str,
        valid_study_locus_path: str,
        invalid_study_locus_path: str,
        trans_qtl_threshold: int,
        invalid_qc_reasons: list[str] = [],
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            study_locus_path (list[str]): Path to study locus dataset.
            study_index_path (str): Path to study index file.
            target_index_path (str): path to the target index.
            valid_study_locus_path (str): Path to write the valid records.
            invalid_study_locus_path (str): Path to write the output file.
            trans_qtl_threshold (int): genomic distance above which a QTL is considered trans.
            invalid_qc_reasons (list[str]): List of invalid quality check reason names from `StudyLocusQualityCheck` (e.g. ['SUBSIGNIFICANT_FLAG']).
        """
        # Reading datasets:
        study_index = StudyIndex.from_parquet(session, study_index_path)
        target_index = TargetIndex.from_parquet(session, target_index_path)

        # Running validation then writing output:
        study_locus_with_qc = (
            StudyLocus.from_parquet(session, list(study_locus_path))
            # Add flag for MHC region
            .qc_MHC_region()
            .validate_chromosome_label()  # Flagging credible sets with unsupported chromosomes
            .validate_study(study_index)  # Flagging studies not in study index
            .annotate_study_type(study_index)  # Add study type to study locus
            .qc_redundant_top_hits_from_PICS()  # Flagging top hits from studies with PICS summary statistics
            .qc_explained_by_SuSiE()  # Flagging credible sets in regions explained by SuSiE
            # Annotates credible intervals and filter to only keep 95% credible sets
            .filter_credible_set(credible_interval=CredibleInterval.IS95)
            # Flagging credible sets with PIP > 1 or PIP < 0.95
            .qc_abnormal_pips(
                sum_pips_lower_threshold=0.95, sum_pips_upper_threshold=1.0001
            )
            # Annotate credible set confidence:
            .assign_confidence()
            # Flagging trans qtls:
            .flag_trans_qtls(study_index, target_index, trans_qtl_threshold)
        ).persist()  # we will need this for 2 types of outputs

        # Valid study locus partitioned to simplify the finding of overlaps
        study_locus_with_qc.valid_rows(invalid_qc_reasons).df.repartitionByRange(
            session.output_partitions, "chromosome", "position"
        ).sortWithinPartitions("chromosome", "position").write.mode(
            session.write_mode
        ).parquet(valid_study_locus_path)

        # Invalid study locus
        study_locus_with_qc.valid_rows(invalid_qc_reasons, invalid=True).df.coalesce(
            session.output_partitions
        ).write.mode(session.write_mode).parquet(invalid_study_locus_path)
