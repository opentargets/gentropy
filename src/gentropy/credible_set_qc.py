"""Step to run credible set quality control on fine-mapping output credible sets."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.method.fine_mapping.credible_set_qc import credible_set_qc


class CredibleSetQCStep:
    """Credible set quality control step for fine-mapped credible sets."""

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        credible_set_variant_path: str,
        credible_set_output_path: str,
        credible_set_variant_output_path: str,
        p_value_threshold: float,
        purity_min_r2: float,
        ld_index_path: str | None,
        study_index_path: str | None,
        ld_min_r2: float,
        n_partitions: int | None,
    ) -> None:
        """Run credible set quality control step.

        Check defaults used by steps in hydra configuration `gentropy.config.CredibleSetQCStepConfig`

        Due to the large number of partitions at the inputs after fine-mapping, both are read
        recursively and coalesced before quality control.

        Giving `ld_index_path` also LD-clumps the credible sets, which requires `study_index_path`.

        Args:
            session (Session): Session object.
            credible_set_path (str): Path to the credible sets.
            credible_set_variant_path (str): Path to the credible set variants.
            credible_set_output_path (str): Path to write the credible sets that pass.
            credible_set_variant_output_path (str): Path to write their variants.
            p_value_threshold (float): Largest lead p-value kept.
            purity_min_r2 (float): Smallest `purityMinR2` kept.
            ld_index_path (str | None): Path to the LD index; when given, credible sets are LD-clumped.
            study_index_path (str | None): Path to the study index, required with `ld_index_path`.
            ld_min_r2 (float): Smallest r2 between two leads that clumps the less significant one.
            n_partitions (int | None): Number of partitions to coalesce the inputs to. Defaults to 200
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
        credible_sets = CredibleSet.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        ).coalesce(n_partitions)
        credible_set_variants = CredibleSetVariant.from_parquet(
            session, credible_set_variant_path, recursiveFileLookup=True
        ).coalesce(n_partitions)

        clean_credible_sets, clean_variants = credible_set_qc(
            credible_sets,
            credible_set_variants,
            p_value_threshold,
            purity_min_r2,
            ld_index,
            study_index,
            ld_min_r2,
        )
        clean_credible_sets.df.write.mode(session.write_mode).parquet(
            credible_set_output_path
        )
        clean_variants.df.write.mode(session.write_mode).parquet(
            credible_set_variant_output_path
        )
