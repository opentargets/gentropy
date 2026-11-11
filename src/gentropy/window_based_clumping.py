"""Step to run window based clumping on summary statistics datasts."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.annotation import annotate_locus
from gentropy.method.clumping.window import WindowClumping


class WindowBasedClumpingStep:
    """Apply window based clumping on summary statistics datasets."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        study_locus_output_path: str,
        distance: int = WindowBasedClumpingStepConfig().distance,
        gwas_significance: float = WindowBasedClumpingStepConfig().gwas_significance,
        collect_locus: bool = WindowBasedClumpingStepConfig().collect_locus,
        inclusion_list_path: str
        | None = WindowBasedClumpingStepConfig().inclusion_list_path,
        recursive_file_lookup: bool = WindowBasedClumpingStepConfig().recursive_file_lookup,
    ) -> None:
        """Run window-based clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
            study_locus_output_path (str): Output path for the resulting study locus dataset.
            distance (int): Base pairs on each side of a lead, both the clumping window and the half-width of each locus. Optional.
            gwas_significance (float): GWAS significance threshold. Defaults to 1e-8.
            collect_locus (bool): Whether to collect the summary statistics inside each locus into `locus`. Optional.
            inclusion_list_path (str | None): Path to the inclusion list (list of white-listed study identifier). Optional.
            recursive_file_lookup (bool): Whether to recursively look for summary statistics files in the input path. Defaults to `True`.
                Note that if an inclusion list is provided, this flag is set to `True` always.

        Check WindowBasedClumpingStepConfig object for default values.
        """
        # If inclusion list path is provided, only these studies will be read:
        if inclusion_list_path:
            study_ids_to_ingest = [
                f"{summary_statistics_input_path}/{row['studyId']}.parquet"
                for row in session.spark.read.parquet(inclusion_list_path).collect()
            ]
            # Force recursive file lookup if inclusion list is provided
            recursive_file_lookup = True
        else:
            # If no inclusion list is provided, read all summary stats in folder:
            study_ids_to_ingest = [summary_statistics_input_path]

        ss = SummaryStatistics.from_parquet(
            session, study_ids_to_ingest, recursiveFileLookup=recursive_file_lookup
        )

        # Clumping:
        study_locus = WindowClumping(
            distance=distance, gwas_significance=gwas_significance
        ).clump(ss)
        # Optional locus collection:
        if collect_locus:
            # annotate_locus reads the loci twice, for the rows and for their bounds;
            # persist them so the summary statistics are clumped once.
            study_locus = study_locus.persist()
            study_locus.df.count()
            study_locus = annotate_locus(study_locus, ss)

        study_locus.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(study_locus_output_path)
