"""Step to run credible set quality control on finemapping output StudyLoci."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.susie_inf import SUSIE_inf


class CredibleSetQCDefaults(BaseModel, frozen=True):
    """Defaults for CredibleSetQCStep.

    All values are frozen - create a new instance to override.
    """

    credible_sets_path: Annotated[str, Field(description="Path to credible sets file.")]
    output_path: Annotated[str, Field(description="Path to write the output file.")]
    p_value_threshold: Annotated[
        float, Field(description="P-value threshold for credible set quality control.")
    ] = 1e-5
    purity_min_r2: Annotated[
        float, Field(description="Minimum R2 for purity estimation.")
    ] = 0.01
    clump: Annotated[
        bool, Field(description="Whether to clump the credible sets by LD.")
    ] = False
    ld_index_path: Annotated[str | None, Field(description="Path to LD index file.")]
    study_index_path: Annotated[
        str | None, Field(description="Path to study index file.")
    ]
    ld_min_r2: Annotated[
        float | None, Field(description="Minimum R2 for LD estimation.")
    ] = 0.8
    n_partitions: Annotated[
        int | None,
        Field(
            description="Number of partitions to coalesce the dataset after reading."
        ),
    ] = 200


class CredibleSetQCStep:
    """Credible set quality control step for fine mapped StudyLoci."""

    def __init__(
        self,
        config: CredibleSetQCDefaults,
        session: Session,
    ) -> None:
        """Run credible set quality control step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        n_partitions = config.n_partitions or 200

        ld_index = (
            LDIndex.from_parquet(session, config.ld_index_path)
            if config.ld_index_path
            else None
        )
        study_index = (
            StudyIndex.from_parquet(session, config.study_index_path)
            if config.study_index_path
            else None
        )

        cred_sets = StudyLocus.from_parquet(
            session, config.credible_sets_path, recursiveFileLookup=True
        ).coalesce(n_partitions)

        cred_sets_clean = SUSIE_inf.credible_set_qc(
            cred_sets,
            config.p_value_threshold,
            config.purity_min_r2,
            config.clump,
            ld_index,
            study_index,
            config.ld_min_r2,
        )
        # ensure the saved object is still a valid StudyLocus
        StudyLocus(
            _df=cred_sets_clean.df, _schema=StudyLocus.get_schema()
        ).df.write.mode(session.write_mode).parquet(config.output_path)
