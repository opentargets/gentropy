"""Step to validate study locus dataset against study index."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.dataset.target_index import TargetIndex


class StudyLocusValidationDefaults(BaseModel, frozen=True):
    """Defaults for StudyLocusValidationStep.

    All values are frozen - create a new instance to override.
    """

    study_locus_path: Annotated[
        list[str], Field(description="Path to study locus dataset.")
    ]
    study_index_path: Annotated[str, Field(description="Path to study index file.")]
    target_index_path: Annotated[str, Field(description="Path to target index file.")]
    valid_study_locus_path: Annotated[
        str, Field(description="Path to write the valid records.")
    ]
    invalid_study_locus_path: Annotated[
        str, Field(description="Path to write the output file.")
    ]
    trans_qtl_threshold: Annotated[
        int,
        Field(description="Genomic distance above which a QTL is considered trans."),
    ]
    invalid_qc_reasons: Annotated[
        list[str] | None,
        Field(
            description="List of invalid quality check reason names from `StudyLocusQualityCheck`."
        ),
    ] = []


class StudyLocusValidationStep:
    """Study index validation step.

    This step reads and outputs a study index dataset with flagged studies
    when target of disease validation fails.
    """

    def __init__(
        self,
        config: StudyLocusValidationDefaults,
        session: Session,
    ) -> None:
        """Initialize step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        invalid_qc_reasons = (
            list(config.invalid_qc_reasons) if config.invalid_qc_reasons else []
        )
        # Reading datasets:
        study_index = StudyIndex.from_parquet(session, config.study_index_path)
        target_index = TargetIndex.from_parquet(session, config.target_index_path)

        # Running validation then writing output:
        study_locus_with_qc = (
            StudyLocus.from_parquet(session, list(config.study_locus_path))
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
                sum_pips_lower_threshold=0.95,
                sum_pips_upper_threshold=1.0001,
            )
            # Annotate credible set confidence:
            .assign_confidence()
            # Flagging trans qtls:
            .flag_trans_qtls(study_index, target_index, config.trans_qtl_threshold)
            .persist()  # we will need this for 2 types of outputs
        )

        result = study_locus_with_qc.valid_rows(invalid_qc_reasons)

        (
            # Valid study locus partitioned to simplify the finding of overlaps
            result.valid.df.repartitionByRange(
                session.output_partitions,
                "chromosome",
                "position",
            )
            .sortWithinPartitions("chromosome", "position")
            .write.mode(session.write_mode)
            .parquet(config.valid_study_locus_path)
        )
        (
            result.invalid.df.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(config.invalid_study_locus_path)
        )
