"""Methods for transforming a `ProteinQuantitativeTraitLocusStudyIndex` into a standard `StudyIndex`."""

from __future__ import annotations

from gentropy import Session, TargetIndex
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex


class pQTLStudyIndexTransformationStep:
    """Transform a `ProteinQuantitativeTraitLocusStudyIndex` into a standard `StudyIndex`.

    This step resolves gene-level and protein-level annotations from the
    `TargetIndex` (e.g. Ensembl gene IDs) and
    writes a study index compatible with the downstream Open Targets genetics pipeline.
    """

    def __init__(
        self,
        session: Session,
        protein_study_index_path: str,
        study_index_path: str,
        target_index_path: str,
    ) -> None:
        """Initialise and execute the pQTL study-index transformation step.

        Args:
            session (Session): Active Gentropy Spark session.
            protein_study_index_path (str): Path to the
                `ProteinQuantitativeTraitLocusStudyIndex`.
            study_index_path (str): Destination path for the resolved
                `StudyIndex` Parquet dataset.
            target_index_path (str): Path to the
                `TargetIndex` Parquet dataset used
                to map gene symbols to Ensembl gene IDs.
        """
        pqtl = ProteinQuantitativeTraitLocusStudyIndex.from_parquet(
            session, protein_study_index_path
        )
        ti = TargetIndex.from_parquet(session, target_index_path)

        s = pqtl.to_study(ti)
        s.df.coalesce(1).write.mode(session.write_mode).parquet(study_index_path)
