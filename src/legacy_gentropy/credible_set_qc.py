"""Step to run credible set quality control on finemapping output StudyLoci."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.susie_inf import SUSIE_inf


class CredibleSetQCStep:
    """Credible set quality control step for fine mapped StudyLoci."""

    def __init__(
        self,
        session: Session,
        credible_sets_path: str,
        output_path: str,
        p_value_threshold: float,
        purity_min_r2: float,
        clump: bool,
        ld_index_path: str | None,
        study_index_path: str | None,
        ld_min_r2: float | None,
        n_partitions: int | None,
    ) -> None:
        """Run credible set quality control step.

        Check defaults used by steps in hydra configuration `gentropy.config.CredibleSetQCStepConfig`

        Due to the large number of partitions at the input credible_set_path after finemapping, the
        best strategy it is to repartition and save the dataset after deduplication.

        The `clump` mode will perform additional LD based clumping on the input credible sets.
        Enabling `clump` mode requires providing `ld_index_path`, `study_index_path` and `ld_min_r2`.

        Args:
            session (Session): Session object.
            credible_sets_path (str): Path to credible sets file.
            output_path (str): Path to write the output file.
            p_value_threshold (float): P-value threshold for credible set quality control.
            purity_min_r2 (float): Minimum R2 for purity estimation.
            clump (bool): Whether to clump the credible sets by LD.
            ld_index_path (str | None): Path to LD index file.
            study_index_path (str | None): Path to study index file.
            ld_min_r2 (float | None): Minimum R2 for LD estimation.
            n_partitions (int | None): Number of partitions to coalesce the dataset after reading. Defaults to 200
        """
        n_partitions = n_partitions or 200

        ld_index = (
            LDIndex.from_parquet(session, ld_index_path) if ld_index_path else None
        )
        study_index = (
            StudyIndex.from_parquet(session, study_index_path)
            if study_index_path
            else None
        )

        cred_sets = StudyLocus.from_parquet(
            session, credible_sets_path, recursiveFileLookup=True
        ).coalesce(n_partitions)

        cred_sets_clean = SUSIE_inf.credible_set_qc(
            cred_sets,
            p_value_threshold,
            purity_min_r2,
            clump,
            ld_index,
            study_index,
            ld_min_r2,
        )
        # ensure the saved object is still a valid StudyLocus
        StudyLocus(
            _df=cred_sets_clean.df, _schema=StudyLocus.get_schema()
        ).df.write.mode(session.write_mode).parquet(output_path)
