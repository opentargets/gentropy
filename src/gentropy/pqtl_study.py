"""Methods for transforming a `ProteinQuantitativeTraitLocusStudyIndex` into a standard `StudyIndex`."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy import Session, TargetIndex
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex


class pQTLStudyIndexTransformationStepConfig(BaseModel, frozen=True):
    """Defaults for pQTLStudyIndexTransformationStep.

    All values are frozen - create a new instance to override.
    """

    protein_study_index_path: Annotated[
        str, Field(description="Path to the ProteinQuantitativeTraitLocusStudyIndex.")
    ]
    study_index_path: Annotated[
        str,
        Field(
            description="Destination path for the resolved StudyIndex Parquet dataset."
        ),
    ]
    target_index_path: Annotated[
        str,
        Field(
            description="Path to the TargetIndex Parquet dataset used to map gene symbols to Ensembl gene IDs."
        ),
    ]


class pQTLStudyIndexTransformationStep:
    """Transform a `ProteinQuantitativeTraitLocusStudyIndex` into a standard `StudyIndex`.

    This step resolves gene-level and protein-level annotations from the
    `TargetIndex` (e.g. Ensembl gene IDs) and
    writes a study index compatible with the downstream Open Targets genetics pipeline.
    """

    def __init__(
        self, session: Session, config: pQTLStudyIndexTransformationStepConfig
    ) -> None:
        """Initialise and execute the pQTL study-index transformation step.

        Args:
            config (pQTLStudyIndexTransformationStepConfig): Configuration for the step.
            session (Session): Active Gentropy Spark session.
        """
        pqtl = ProteinQuantitativeTraitLocusStudyIndex.from_parquet(
            session, config.protein_study_index_path
        )
        ti = TargetIndex.from_parquet(session, config.target_index_path)

        s = pqtl.to_study(ti)
        s.df.coalesce(1).write.mode(session.write_mode).parquet(config.study_index_path)
